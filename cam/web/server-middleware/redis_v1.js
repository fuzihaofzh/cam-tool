import Redis from "ioredis"
import fs from 'fs'
import YAML from 'yaml'
import os from 'os'
import process from 'process'
import querystring from 'querystring'

const conf = fs.readFileSync('/Users/maple/.cam.conf', 'utf8')
var user_config = YAML.parse(conf)
const redis_url = `redis://:${user_config['password']}@${user_config['server']}:${user_config['port']}`


export default async function (req, res, next) {
    // req is the Node.js http request object
    const client = new Redis(redis_url);
    const host = os.hostname()
    const node = `${host}-${process.pid}`
    const parms = querystring.parse(req.url)
    console.log(parms)
    if (parms['/?type'] == 'status'){
        console.log("Getting status")
        client.lpush('to_server', `{"type" : "GET_STATUS", "node" : "${node}"}`)
        client.brpop(`to_${node}`, 5.0).then(message => {
            res.write(message[1]);
            res.end();
            next();
        })
    }else if (parms['/?type'] == 'kill'){
        console.log("In kill")
        client.lpush('to_server', `{"type" : "KILL", "task_id" : ${parms.tid}, "node" : "${node}"}`)
    }else if (parms['/?type'] == 'stdout'){
        console.log("In stdout")
        client.lpush('to_server', `{"type" : "STDOUT", "task_id" : ${parms.tid}, "node" : "${node}"}`)
        client.brpop(`to_${node}`, 5.0).then(message => {
            console.log(message[1])
            res.write(message[1]);
            res.end();
            next();
        })
    }else if (parms['/?type'] == 'run'){
        var cmd = decodeURI(parms.cmd)
        var stxt = `{"type" : "ADD", "node" : "${node}", "cmd" : "${cmd}", "host" : "${host}"}`
        console.log("Run command:", stxt)
        client.lpush('to_server', stxt)
    }
  }