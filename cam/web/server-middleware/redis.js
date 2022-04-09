import Redis from "ioredis"
import fs from 'fs'
import YAML from 'yaml'
import os from 'os'
import process from 'process'
import querystring from 'querystring'
const WebSocket = require('ws');

const conf = fs.readFileSync('/Users/maple/.cam.conf', 'utf8')
var user_config = YAML.parse(conf)
const redis_url = `redis://:${user_config['password']}@${user_config['server']}:${user_config['port']}`
const server = new WebSocket.Server({ port : user_config['port'] + 1 });

function notification(){
    // Create & Start the WebSocket server
    console.log("In notification")
    const client = new Redis(redis_url);
    client.subscribe(["__keyspace@0__:task_pending", "__keyspace@0__:task_running"])//, "__keyspace@0__:node_list"
    // Register event for client connection
    server.on('connection', function connection(ws) {
        // broadcast on web socket when receving a Redis PUB/SUB Event
        client.on('message', function(channel, message){
            console.log("Receive notification")
            ws.send(channel);
        })
    });
}
notification()


export default async function (req, res, next) {
    // req is the Node.js http request object
    // We must have res.end(); next(); in the end!!!
    console.log("In redis")
    const parms = querystring.parse(req.url)
    console.log(parms)
    const client = new Redis(redis_url);
    const host = os.hostname()
    const node = `${host}-${process.pid}`
    if (parms['/?type'] == 'status'){
        
        console.log("Getting status")
        /*var msg = { 'pending' : await client.lrange('task_pending', 0, -1),
                'running' : await client.hgetall('task_running'),
                'finished': await client.hgetall('task_finished'),
                'nodes' : await client.hgetall('node_list'),
            }
        var pending = {}
        for(var p of msg['pending']){
            pending[p['task_id']] = p;
        }*/
        var pipeline = client.pipeline();
        var node_list = await client.hgetall('node_list');
        pipeline.lrange('task_pending', 0, -1)
        pipeline.hgetall('task_running')
        pipeline.hgetall('task_finished')
        for (var nname in node_list){
            var nd = JSON.parse(node_list[nname])
            pipeline.lrange("task_pending_" + nd['host'], 0, -1)
        }
        pipeline.exec(function (err, results) {
            var pending = {}
            for(var p of results[0][1]){
                console.log(p)
                var tid = JSON.parse(p)['task_id']
                pending[tid] = p;
            }
            for(var i = 3; i < results.length; ++i){
                for(var p of results[i][1]){
                    var tid = JSON.parse(p)['task_id']
                    pending[tid] = p;
                }
            }
            var msg = {
                'pending' : pending,
                'running' : results[1][1],
                'finished': results[2][1],
                'nodes'   : node_list,
            }
            res.write(JSON.stringify(msg));
            res.end();
            next();
        });
        
        /*var p1 = client.lrange('task_pending', 0, -1)
        var p2 = client.hgetall('task_running')
        var p3 = client.hgetall('task_finished')
        Promise.all([p1, p2, p3, p4]).then(values => {
            var pending = {}
            for(var p of values[0]){
                var tid = JSON.parse(p)['task_id']
                pending[tid] = p;
            }
            var msg = {
                'pending' : pending,
                'running' : values[1],
                'finished': values[2],
                'nodes'   : node_list[3],
            }
            msg['pending'] = pending
            res.write(JSON.stringify(msg));
            res.end();
            next();
          });*/
          console.log("End status")
    }else if (parms['/?type'] == 'kill'){
        console.log("In kill")
        console.log(parms)
        async function rm_task(part){
            var pending = await client.lrange(part, 0, -1)
            for (var idx in pending){
                task = JSON.parse(pending[idx])
                if (task['task_id'] == parms.tid){
                    client.lrem(part, 1, pending[idx])
                    task['status'] = 'CANCELED';
                    var tsp = new Date().toISOString();
                    tsp = tsp.split('.')[0].replace('T', ' ')
                    task['end_time'] = tsp;
                    client.hset('task_finished', task['task_id'], JSON.stringify(task))
                }
            }
        }
        rm_task('task_pending')
        rm_task(`task_pending_${parms.host}`)
        var task = await client.hget('task_running', parms.tid)
        if (task != null){
            task = JSON.parse(task)
            client.lpush(`to_${task['node']}`, `{"type":"KILL", "task_id" : ${task["task_id"]}}`)
        }
        res.end();
        next();
    }else if (parms['/?type'] == 'stdout'){
        console.log("In stdout")
        await client.lpush(`to_${parms.node}`, `{"type" : "STDOUT", "task_id" : ${parms.tid}}`)
        await setTimeout(function() {}, 1000);
        var log = await client.hget("task_log", parms.tid)
        res.write(JSON.stringify({"stdout":log}));
        res.end();
        next();
    }else if (parms['/?type'] == 'run'){
        Promise.all([client.hgetall("node_list"), client.get('jobid')]).then(values => {
            var node_list = values[0]
            var sid = values[1]
            var mid = 0
            for(var nd in node_list){
                var ninfo = JSON.parse(node_list[nd])
                if (ninfo['task']['task_id'] > mid) mid = ninfo['task']['task_id']
            }
            var tid = Math.max(mid, sid) + 1
            var cmd = decodeURI(parms.cmd)
            var tsp = new Date().toISOString();
            tsp = tsp.split('.')[0].replace('T', ' ')
            var ntask = `{"cmd" : "${cmd}", "submit_time" : "${tsp}", "task_id" : ${tid}}`
            if(parms.sel_host != ""){
                ntask = `{"cmd" : "${cmd}", "submit_time" : "${tsp}", "task_id" : ${tid}, "host" : "${parms.host}"}`
            }
            if (parms.host == ""){
                client.lpush("task_pending", ntask)
            }else{
                client.lpush(`task_pending_${parms.host}`, ntask)
            }
            client.set('jobid', tid)
            res.end();
            next();
        })
    }
  }