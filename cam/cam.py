#!/usr/bin/env python
import multiprocessing
import fire
import os
import yaml
import redis
import json
import time
import socket
import subprocess
import signal
import psutil
import collections
import sys
import io
from pathlib import Path
from subprocess import Popen, PIPE
from cam.version import __version__
import datetime
import errno
import functools
import traceback
HOME = str(Path.home())
CONFIG_FILE = "{0}/.cam.conf".format(HOME)
DEFAULT_CONF="""server: 127.0.0.1
port: 3857
password: 0a8148539c426d7c008433172230b551
host_lock_time: 60
"""

def get_time():
    return str(datetime.datetime.utcnow()).split('.')[0]

def get_node_name():
    return get_host_name() + "-" + str(os.getpid())

def get_host_name():
    return socket.gethostname().split('.', 1)[0]

def time_diff(now, st):
    return str(now - st).split('.')[0].replace(' day, ', '-').replace(' days, ', '-')

def get_seconds_passed(tm):
    return (datetime.datetime.utcnow() - datetime.datetime.fromisoformat(tm)).seconds

def _log(info, color):
    csi = '\033['
    colors = {
    "red" : csi + '31m',
    "green" : csi + '32m',
    "yellow" : csi + '33m',
    "blue" : csi + '34m'
    }
    end = csi + '0m'
    print("{0}[CAM {1}] {2} ".format(colors[color], get_time()[2:], end), info)

def log_info(*args):
    _log("".join([str(a) for a in args]), "blue")

def log_warn(*args):
    _log("".join([str(a) for a in args]), "red")

def bash(cmd):
    return subprocess.getoutput(cmd)

def ngpu(maxmem = 30):# Max used memory in Mb
    import GPUtil
    gpus = GPUtil.getGPUs()
    return len([g for g in gpus if g.memoryUsed < maxmem])

def nsnode(*nodes):#Slurm Node Count
   return sum([bash('squeue').count(s) for s in nodes])

def parse_json(data):
    return json.loads(data)

def kill_subs():
    parent = psutil.Process(os.getpid())
    try:
        for child in parent.children(recursive=True):
            for i in range(3):
                child.send_signal(signal.SIGINT)
                time.sleep(0.3)
    except Exception as e:
        return

def run_cmd(cmd, log_queue):
    try:
        proc = Popen(cmd, shell=True, stdout = PIPE, stderr=subprocess.STDOUT, bufsize=0)
        for content in proc.stdout:
            content = content.decode("utf-8")
            print(content, end = "")
            log_queue.put(content)
    except Exception as e:
        return 



class CAM(object):
    def __init__(self):
        self.__version__ = __version__
        if not os.path.exists(CONFIG_FILE):
            open(CONFIG_FILE, "w").write(DEFAULT_CONF)
        self._conf = yaml.load(open(CONFIG_FILE).read(), yaml.FullLoader) 
        self._redis = redis.StrictRedis(host=self._conf["server"], port=self._conf["port"], password=self._conf["password"], db=0, encoding="utf-8", decode_responses=True)
        self._channels = {}
        self._log_queue = {}
        self._log = {}
        self._server_fails = False
        self._host_lock = {}

    def __del__(self):
        kill_subs()  

    def _log_sys_info(self):
        log_info("Server: ", self._conf['server'], ":", str(self._conf['port']), ' v', self.__version__, " node: ", get_node_name())           

    def _condition_parse(self, cond):
        #e.g.:
        #Has Free GPU   : "bash('nvidia-smi').count(' 0MiB /') > 2"
        #Slurm job count: "int(bash('squeue -h -t pending,running -r | wc -l')) < 4"
        #Slurm node count: "bash('squeue').count('ltl-gpu')<4"
        if cond == "":
            return True
        else:
            return eval(str(cond))
    """
    def _publish(self, channel, msg):
        try:
            self._redis.lpush(channel, json.dumps(msg))
            #self._redis.publish(channel, json.dumps(msg))
            if self._server_fails:
                self._log_sys_info()
                self._server_fails = False
        except Exception as e:
            if not self._server_fails:
                log_warn(e)
                self._server_fails = True

    def _get_message(self, channel):
        try:
            msg = self._redis.brpop(channel, timeout = 5)
            msg = msg[1] if msg is not None else msg
            if self._server_fails:
                self._log_sys_info()
                self._server_fails = False
            return json.loads(msg) if msg is not None else None
        except Exception as e:
            if not self._server_fails:
                log_warn(e)
                self._server_fails = True
    

    def _make_worker_msg(self, **kwargs):
        msg = {"type" : "STATUS", "node" : get_node_name(), "host" : get_host_name(), "time": get_time(), "pwd" : os.getcwd(), "resource": 1, "version": self.__version__}
        for k in self._status:
            kwargs[k] = self._status[k]
        msg.update(kwargs)
        return msg

    def _make_server_msg(self, **kwargs):
        msg = {"time": get_time()}
        for k in self._status:
            kwargs[k] = self._status[k]
        msg.update(kwargs)
        return msg
    
    def _server_handle_tick(self):
        for node in list(self.node_list.keys()):
            if 300 > get_seconds_passed(self.node_list[node]['time']) > 60:
                self.node_list[node]['status'] = "DISCONNECTED"
            elif get_seconds_passed(self.node_list[node]['time']) > 300:
                del self.node_list[node]
        for task in list(self.task_running.keys()):
            if get_seconds_passed(self.task_running[task]['time']) > 60:
                self.task_running[task]['status'] = "DISCONNECTED"
                self.task_finished[task] = self.task_running[task]
                del self.task_running[task]
        for task_id, task in self.task_pending.items():
            nodes = sorted(self.node_list.values(), key=lambda x: -x['priority'])
            nodes = [n for n in nodes if (n['host'] not in self._host_lock or (time.time() - self._host_lock[n['host']] > self._conf['host_lock_time'])) and n['resource'] > 0]
            for node in nodes:
                nid = node['node']
                #print(task_id, task, nid, node)
                if ('runhost' not in task or task['runhost'] is None or (node['host'] in task['runhost'])) and node['status'] in ['IDLE', 'FINISHED']:
                    msg = self._make_server_msg(type = "RUN", cmd = task['cmd'], task_id = task_id)
                    self._publish("to_%s"%nid, msg)
                    task['assigned_time'] = get_time()
                    log_info("Assign task %d to node %s."%(task_id, nid))
                    log_info(str(msg))
                    self._host_lock[node['host']] = time.time()
                    break
        for lst in [self.task_pending, self.task_running]:
            for task_id, task in lst.items():
                if task['status'] == 'KILLED':
                    self.task_finished[task_id] = task
                    del lst[task_id]

    def _server_handle_message(self, msg):
        if msg is None:
            return
        log_info(msg)
        if msg['type'] == 'ADD':
            self.task_pending[self.task_cnt] = msg
            self.task_pending[self.task_cnt]['task_id'] = self.task_cnt
            self.task_pending[self.task_cnt]['status'] = "PENDING"
            log_info("Add new task: ", str(self.task_pending[self.task_cnt]))
            self.task_cnt += 1
        elif msg['type'] == 'KILL':
            for tid, task in self.task_pending.items():
                if tid == msg['task_id']:
                    log_info("Remove task %d in pending list." % tid)
                    task['status'] = "KILLED"
            for tid, task in self.task_running.items():
                if tid == msg['task_id']:
                    log_info("Killing running task %d ." % tid)
                    msg = self._make_server_msg(type = "KILL", task_id = tid)
                    self._publish("to_%s"%task['node'], msg)
        elif msg['type'] == 'WORKER_TASK_KILLED':
            log_info("Task %d has been killed successfully." % msg['task_id'])
            if msg['task_id'] in self.task_running:
                self.task_running[msg['task_id']]['status'] = 'KILLED'
        elif msg['type'] == 'STDOUT':
            log_info(msg, self.task_running)
            for lst in [self.task_running, self.task_finished]:
                if msg['task_id'] in lst:
                    log_info("Get stdout of task %d"%msg['task_id'])
                    self._publish("to_%s"%lst[msg['task_id']]['node'], self._make_server_msg(type = "STDOUT", task_id = msg['task_id']))
                    self.ask_node[lst[msg['task_id']]['node']] = msg['node']
        elif msg['type'] == 'STDOUT_RES':
            log_info("Receive stdout of task %s. Send it to %s"%(msg['node'], self.ask_node[msg['node']]))
            self._publish("to_%s"%self.ask_node[msg['node']], self._make_server_msg(type = "STDOUT_RES", stdout_res = msg['stdout_res']))
        elif msg['type'] == 'GET_STATUS':
            log_info("Receive GET_STATUS request.")
            status = self._make_server_msg(type = "SERVER_STATUS", status = {"running" : dict(self.task_running), "pending" : dict(self.task_pending), "finished" : dict(self.task_finished), "nodes" : dict(self.node_list)})
            self._publish("to_%s"%msg['node'], status)
            log_info(status)
            log_info("to_%s"%msg['node'])
            log_info("STATUS sent.")
        elif msg['type'] == 'STATUS':
            self.node_list[msg['node']] = msg
            if msg['status'] == 'RUNNING':
                if msg['task_id'] in self.task_pending:
                    del self.task_pending[msg['task_id']]
                self.task_running[msg['task_id']] = msg
            if msg['status'] == 'FINISHED':
                if msg['task_id'] in self.task_running:
                    self.task_finished[msg['task_id']] = self.task_running[msg['task_id']]
                    self.task_finished[msg['task_id']]['finish_time'] = msg['time']
                    self.task_finished[msg['task_id']]['status'] = "FINISHED"
                    del self.task_running[msg['task_id']]
                    log_info("Task %d finished."%(msg['task_id']))
                    log_info(str(msg))
            elif msg['status'] == 'IDLE':
                #self._publish("to_%s"%msg['node'], self._make_server_msg(type = "RUN", cmd = "./test.sh", task_id = 0))
                pass
    """    
    def server(self, port = None, password = None):
        """
        Start the server.
        """
        self._conf['password'] = password if password is not None else self._conf['password']
        self._conf['port'] = port if port is not None else self._conf['port']
        port = self._conf["port"] if port is None else port
        self._log_sys_info()
        os.system("redis-server --port {0} --requirepass {1}".format(port, self._conf["password"]))
        """self.task_pending = collections.OrderedDict()
        self.task_running = collections.OrderedDict()
        self.task_finished = collections.OrderedDict()
        self.node_list = collections.OrderedDict()
        self.ask_node = {}
        self.task_cnt = 0
        self._log_sys_info()
        while True:
            self._server_handle_tick()
            msg = self._get_message("to_server")
            self._server_handle_message(msg)"""
            
    def _hset(self, var_name, key_name, dct):
        self._redis.hset(var_name, key_name, json.dumps(dct))

    def _get_by_tid(self, part, tid):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [parse_json(d) for d in part_str]
        for i in range(len(pending)):
            if pending[i]["task_id"] == tid:
                return pending[i]
        return None

    def _remove_by_tid(self, part, tid):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [parse_json(d) for d in part_str]
        for i in range(len(pending)):
            if pending[i]["task_id"] == tid:
                self._redis.lrem(part, 1, part_str[i])
                return part_str[i]

    def _check_host_lock(self, wait = 5):
        dt = self._redis.hget("worker_lock", get_host_name())
        if dt is not None:
            now = datetime.datetime.utcnow()
            dt = datetime.datetime.fromisoformat(dt.decode('utf-8'))
            if (now - dt).seconds < wait:
                return True
        return False

    def _update_node_status(self):
        self._node_status['resource'] = self._condition_parse(self._resource_cond)
        if self._node_status['node_status'] == "RUNNING":
            pass
        elif self._node_status['resource'] <= 0:
            self._node_status['node_status'] = "WAIT RESOURCE"
        elif self._check_host_lock(self._node_status['host_lock_time']):
            self._node_status['node_status'] = "WAIT LOCK"
        else:
            self._node_status['node_status'] = "IDLE"
        self._node_status['timestamp'] = get_time()
        self._hset("node_list", get_node_name(), self._node_status)

    def _get_hlist(self, hname):
        ptable = {k : json.loads(v) for k, v in self._redis.hgetall(hname).items()}
        return ptable

    def _get_message(self, timeout = 10):
        try:
            self._update_node_status()
            msg = self._watcher.get_message(timeout = timeout)
            len_task_pending = self._redis.llen("task_pending")
            len_to_node = self._redis.llen(f"to_{get_node_name()}")
            if msg == None and len_task_pending == 0 and len_to_node == 0:
                return None
            if len_to_node != 0:
                return json.loads(self._redis.rpop(f"to_{get_node_name()}"))
            if len_task_pending != 0 and not self._node_status["node_status"] in ["RUNNING", "WAIT RESOURCE"]:
                node_list = self._get_hlist("node_list")
                prior = [e for e in node_list if node_list[e]["node_status"] in ["IDLE", "FINISHED"] and node_list[e]["priority"] > self._node_status["priority"]]
                if len(prior) > 0:
                    return None
                return {"type" : "RUN", "task" : json.loads(self._redis.rpop("task_pending"))}
            if self._server_fails:
                self._log_sys_info()
                log_info("Connection Recovered!")
                self._server_fails = False
        except Exception as e:
            if not self._server_fails:
                log_warn(e)
                traceback.print_exc()
                self._server_fails = True
            return None

    
    def worker(self, resource=1, prefix="", priority=10, suffix = "", server = None, port = None, host_lock_time=30):
        """
         Start the worker. 
        <br>`cam worker "some start condition"`
        <br>Start condition can be specified with bash and python e.g.: 
        <br>Has Free GPU\t: "bash('nvidia-smi').count(' 0MiB /') > 2"
        <br>Also use\t: "ngpu() > 2"
        <br>Slurm job count\t: "int(bash('squeue -h -t pending,running -r | wc -l')) < 4"
        <br>Slurm node count\t: "bash('squeue').count('node1')<4"
        <br>Also use\t: "nsnode("node1", "node2") < 2"
        <br>`cam worker "some start condition" prefix suffix` will add prefix and suffix to the command.
        """
        self._conf['server'] = server if server is not None else self._conf['server']
        self._conf['port'] = port if port is not None else self._conf['port']
        if server is not None or port is not None:
            self._redis = redis.StrictRedis(host=self._conf["server"], port=self._conf["port"], password=self._conf["password"], db=0, encoding="utf-8")
        self._log_sys_info()
        self._node_status = {"node" : get_node_name(), "host" : get_host_name(), "priority" : priority, "prefix": prefix, "suffix": suffix, "node_status" : "IDLE", "host_lock_time" : host_lock_time}
        self._redis.config_set("notify-keyspace-events", "KEA")
        self._watcher = self._redis.pubsub()
        self._watcher.subscribe(["__keyspace@0__:task_pending", f"__keyspace@0__:to_{get_node_name()}"])
        self._resource_cond = resource
        os.system("tmux rename-window cam%d"%os.getpid())
        while True:
            msg = self._get_message()
            if self._node_status['node_status'] == "RUNNING":
                if not self.p.is_alive():
                    self._node_status['task']['end_time'] = get_time()
                    self._node_status['node_status'] = "FINISHED"
                    self._update_node_status()
                    txt = ""
                    while not self._log_queue[self._node_status['task']['task_id']].empty():
                        txt = self._log_queue[self._node_status['task']['task_id']].get_nowait()
                        self._log[self._node_status['task']['task_id']] += txt
                    log_info(f"Finished running task {self._node_status['task']['task_id']}.")
                    self._log_sys_info()
            if msg is None:
                continue
            log_info(msg)
            if msg['type'] == 'RUN' and self._node_status['node_status'] != "RUNNING":
                task = msg['task']
                self._log_queue[task['task_id']] = multiprocessing.Queue()
                self.p = multiprocessing.Process(target = run_cmd, args = (prefix + task['cmd'] + suffix, self._log_queue[task['task_id']]))
                self.p.start()
                task_status = {}
                task_status['cmd'] = task['cmd']
                task_status['start_time'] = get_time()
                task_status['task_id'] = task['task_id']
                task_status['node'] = get_node_name()
                task_status['host'] = get_host_name()
                self._node_status['node_status'] = "RUNNING"
                self._node_status['task'] = task_status
                self._redis.hset('task_running', task_status['task_id'], json.dumps(task_status))
                self._redis.hset("worker_lock", get_host_name(), get_time())
                self._remove_by_tid("task_pending", task_status['task_id'])
                self._update_node_status()
                self._log[task['task_id']] = ""
                log_info(f"Start running task {self._node_status['task']['task_id']}.")
            if msg['type'] == 'KILL':
                if msg['task_id'] == self._node_status['task']['task_id']:
                    task =  json.loads(self._redis.hget("task_running", msg['task_id']))
                    task['status'] = "KILLED"
                    self._hset("task_finished", msg['task_id'], task)
                    rrow = self._redis.hdel("task_running", msg['task_id'])
                    log_warn(f"Killing running task {msg['task_id']} .")
                    self._node_status['node_status'] = "KILLING"
                    kill_subs()
                    log_warn("Task ", str(msg['task_id']), " has been killed.")
                    self._node_status["node_status"] = "IDLE"
            elif msg['type'] == "STDOUT":
                txt = ""
                while not self._log_queue[msg['task_id']].empty():
                    txt = self._log_queue[msg['task_id']].get_nowait()
                    self._log[msg['task_id']] += txt
                lines = self._log[msg['task_id']].split('\n')
                maxlen = 300
                if len(lines) < maxlen:
                    txt = self._log[msg['task_id']]
                else:
                    txt = '\n'.join(lines[:maxlen//2]) + "\n\n..........\n\n" + '\n'.join(lines[-maxlen//2:])
                self._redis.hset("task_log", msg['task_id'], txt)



    """
    def worker_v1(self, resource=1, prefix="", priority=10, suffix = "", server = None, port = None):
        self._log_sys_info()
        self._status['status'] = "IDLE"
        self.tick_time = datetime.datetime.utcnow()
        self._status['priority'] = priority
        self._conf['server'] = server if server is not None else self._conf['server']
        self._conf['port'] = port if port is not None else self._conf['port']
        if server is not None or port is not None:
            self._redis = redis.StrictRedis(host=self._conf["server"], port=self._conf["port"], password=self._conf["password"], db=0, encoding="utf-8")
        #self.userout = UserStdout()
        #sys.stdout = self.userout
        #sys.stderr = self.userout
        while True:
            msg = self._get_message("to_%s"%get_node_name())
            self._publish("to_server", self._make_worker_msg())
            if self._status['status'] == "RUNNING":
                if not self.p.is_alive():
                    self._status['end_time'] = get_time()
                    self._status['status'] = "FINISHED"
                    txt = ""
                    while not self._log_queue[self._status['task_id']].empty():
                        txt = self._log_queue[self._status['task_id']].get_nowait()
                        self._log[self._status['task_id']] += txt
                    self._log_sys_info()
            else:
                self._status['resource'] = self._condition_parse(resource)
                if self._status['resource'] == 0:
                    self._status['status'] = "WAIT RESOURCE"
            if msg is not None:
                log_info(msg)
            if msg is None:
                continue
            elif msg['type'] == "RUN" and self._status['status'] != "RUNNING":
                #self.p = Popen(msg['cmd'], shell=True, stdout = PIPE, stderr=PIPE, bufsize=0)
                self._log_queue[msg['task_id']] = multiprocessing.Queue()
                self.p = multiprocessing.Process(target = run_cmd, args = (prefix + msg['cmd'] + suffix, self._log_queue[msg['task_id']]))
                self.p.start()
                self._status['status'] = "RUNNING"
                self._status['cmd'] = msg['cmd']
                self._status['start_time'] = get_time()
                self._status['task_id'] = msg['task_id']
                self._status['node'] = get_node_name()
                self._log[msg['task_id']] = ""
            elif msg['type'] == "KILL":
                if self._status['task_id'] == msg['task_id']:
                    log_warn("Killing running task %d ." % self._status['task_id'])
                    self._status['status'] = "KILLING"
                    self._publish("to_server", self._make_worker_msg())
                    kill_subs()
                    log_warn("Task ", str(msg['task_id']), " has been killed.")
                    log_info(msg)
                    self._publish("to_server", self._make_worker_msg(type = 'WORKER_TASK_KILLED', task_id = self._status['task_id']))
                    self._status = {"status" : "IDLE"}
            elif msg['type'] == "REPORT":
                res = eval(msg['cmd'])
                self._publish("to_server", self._make_worker_msg(type = 'REPORT', res = res))
            elif msg['type'] == "STDOUT":
                txt = ""
                while not self._log_queue[msg['task_id']].empty():
                    txt = self._log_queue[msg['task_id']].get_nowait()
                    self._log[msg['task_id']] += txt
                lines = self._log[msg['task_id']].split('\n')
                maxlen = 300
                if len(lines) < maxlen:
                    txt = self._log[msg['task_id']]
                else:
                    txt = '\n'.join(lines[:maxlen//2]) + "\n\n..........\n\n" + '\n'.join(lines[-maxlen//2:])
                self._publish("to_server", self._make_worker_msg(type = 'STDOUT_RES', stdout_res = txt))
    

    def add(self, cmd):
        self._publish("to_server", self._make_worker_msg(type = 'ADD', cmd = cmd, host = None))
    """

    def add(self, cmd):
        """
        Add a new task.
        """
        node_list = self._get_hlist("node_list")
        tcnt = int(self._redis.get('jobid') or 0)
        tid = max([node_list[n]["task"]['task_id'] for n in node_list if "task" in node_list[n] and "task_id" in node_list[n]["task"]] + [tcnt]) + 1
        self._redis.lpush("task_pending", json.dumps({"cmd" : cmd, "submit_time" : get_time(), "task_id" : tid}))
        log_info(f"New Task: {tid}")
        self._redis.set('jobid', f"{tid}")

    def kill(self, rid):
        """
        kill task by its id. e.g. 
        <br>`cam kill 2`
        """
        prow = self._get_by_tid("task_pending", rid)
        if prow is not None:
            log_warn(f"Task {rid} has been removed")
            log_warn(self._remove_by_tid("task_pending", rid))
            return 
        task = json.loads(self._redis.hget("task_running", rid))
        if task is not None:
            self._redis.lpush(f"to_{task['node']}", json.dumps({'type':'KILL', 'task_id' : task['task_id']}))
            log_warn(f"Task {rid} has been killed.")
            log_warn(task)
            return 
        log_info(f"Task {rid} no found.")

    def log(self, tid):
        task = json.loads(self._redis.hget("task_running", tid) or self._redis.hget("task_finished", tid))
        self._redis.lpush(f"to_{task['node']}", json.dumps({'type':'STDOUT', 'task_id' : task['task_id']}))
        time.sleep(1)
        stdout = self._redis.hget("task_log", tid)
        log_info(stdout)

    def ls(self):
        self._log_sys_info()
        pending = self._redis.lrange("task_pending", 0, -1)
        running = self._redis.hgetall("task_running")
        finished = self._redis.hgetall("task_finished")
        nodes = self._redis.hgetall("node_list")
        log_info("Pending:", pending)
        log_info("Running:", running)
        log_info("Finished:", finished)
        log_info("Nodes:", nodes)
        

    def config(self):
        """
        Edit the config file ~/.cam.conf
        """
        os.system("vim {0}".format(CONFIG_FILE))

def main():
    Cam = CAM()
    fire.Fire(Cam)

if __name__ == '__main__':
    main()