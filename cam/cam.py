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
lock_time: 60
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

def ngpu(maxmem = 100):# Max used memory in Mb
    import GPUtil
    gpus = GPUtil.getGPUs()
    return len([g for g in gpus if g.memoryUsed < maxmem])

def nsnode(*nodes):#Slurm Node Count
   return sum([bash('squeue').count(s) for s in nodes])

def parse_json(data):
    return json.loads(data, strict=False)

def kill_subs():
    parent = psutil.Process(os.getpid())
    try:
        for child in parent.children(recursive=True):
            for i in range(3):
                child.send_signal(signal.SIGINT)
                time.sleep(0.3)
    except Exception as e:
        return

def run_cmd(cmd, log_queue, node_name, conf):
    try:
        proc = Popen(cmd, shell=True, stdout = PIPE, stderr=subprocess.STDOUT, bufsize=0)
        for content in proc.stdout:
            content = content.decode("utf-8")
            print(content, end = "")
            log_queue.put(content)
    except Exception as e:
        return 
    _redis = redis.StrictRedis(host=conf["server"], port=conf["port"], password=conf["password"], db=0, encoding="utf-8", decode_responses=True)
    _redis.lpush(f"to_{node_name}", json.dumps({'type':'TASK_FINISHED', 'task_id' : -1}))



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
        self._redis.hdel("node_list", get_node_name())

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
    
    def server(self, port = None, password = None):
        """
        Start the server.
        """
        self._conf['password'] = password if password is not None else self._conf['password']
        self._conf['port'] = port if port is not None else self._conf['port']
        port = self._conf["port"] if port is None else port
        self._log_sys_info()
        os.system("redis-server --port {0} --requirepass {1}".format(port, self._conf["password"]))
            
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
            dt = datetime.datetime.fromisoformat(dt)
            if (now - dt).seconds < wait:
                return True
        return False

    def _update_node_status(self):
        self._node_status['resource'] = self._condition_parse(self._resource_cond)
        if self._node_status['node_status'] == "RUNNING":
            pass
        elif self._node_status['resource'] <= 0:
            self._node_status['node_status'] = "WAIT RESOURCE"
        elif self._check_host_lock(self._node_status['lock_time']):
            self._node_status['node_status'] = "WAIT LOCK"
        else:
            self._node_status['node_status'] = "IDLE"
        self._node_status['timestamp'] = get_time()
        node_list = self._get_hlist("node_list")
        now = datetime.datetime.utcnow()
        for node in node_list:
            dt = datetime.datetime.fromisoformat(node_list[node]['timestamp'])
            if (now - dt).seconds > 300:
                self._redis.hdel("node_list", node)
        self._hset("node_list", get_node_name(), self._node_status)

    def _check_disconnected_task(self):
        running = self._redis.hgetall("task_running")
        node_list = self._get_hlist("node_list")
        for tid in running:
            task = json.loads(running[tid])
            if task['node'] not in node_list:
                task['end_time'] = get_time()
                task['status'] = "DISCONNECTED"
                self._hset("task_finished", task['task_id'], task)
                self._redis.hdel("task_running", task['task_id'])

    def _get_hlist(self, hname):
        ptable = {k : json.loads(v) for k, v in self._redis.hgetall(hname).items()}
        return ptable

    def _get_message(self, timeout = 10):
        try:
            len_to_node = self._redis.llen(f"to_{get_node_name()}")
            if len_to_node != 0:
                msg = self._redis.rpop(f"to_{get_node_name()}")
                if msg is not None:
                    return json.loads(msg)
            msg = self._watcher.get_message(timeout = timeout)
            len_task_pending = self._redis.llen("task_pending")
            if msg == None:
                self._update_node_status()
                self._check_disconnected_task()
            if msg == None and len_task_pending == 0 and len_to_node == 0:
                return None
            if len_task_pending != 0 and not self._node_status["node_status"] in ["RUNNING", "WAIT RESOURCE", "WAIT LOCK"]:
                node_list = self._get_hlist("node_list")
                prior = [e for e in node_list if node_list[e]["node_status"] in ["IDLE", "FINISHED"] and (node_list[e]["priority"] > self._node_status["priority"] or (e > self._node_status["node"] and node_list[e]['host'] == self._node_status['host']))]
                if len(prior) > 0:
                    return None
                task = self._redis.rpop("task_pending")
                if task is None:
                    return None
                task = json.loads(task)
                return {"type" : "RUN", "task" : task}
            if self._server_fails:
                self._log_sys_info()
                log_info("Connection Recovered!")
                self._server_fails = False
        except Exception as e:
            if not self._server_fails:
                traceback.print_exc()
                log_warn(e)
                self._server_fails = True
            return None

    def _handle_task_finished(self):
        self._node_status['task']['end_time'] = get_time()
        self._node_status['task']['status'] = "FINISHED"
        self._node_status['node_status'] = "FINISHED"
        self._hset("task_finished", self._node_status['task']['task_id'], self._node_status['task'])
        self._redis.hdel("task_running", self._node_status['task']['task_id'])
        self._update_node_status()
        txt = ""
        while not self._log_queue[self._node_status['task']['task_id']].empty():
            txt = self._log_queue[self._node_status['task']['task_id']].get_nowait()
            self._log[self._node_status['task']['task_id']] += txt
        log_info(f"Finished running task {self._node_status['task']['task_id']}.")
        self._log_sys_info()

    
    def worker(self, resource=1, prefix="", priority=10, suffix = "", server = None, port = None, lock_time=20):
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
        try:
            if server is not None or port is not None:
                self._redis = redis.StrictRedis(host=self._conf["server"], port=self._conf["port"], password=self._conf["password"], db=0, encoding="utf-8")
            self._log_sys_info()
            self._node_status = {"node" : get_node_name(), "host" : get_host_name(), "priority" : priority, "prefix": prefix, "suffix": suffix, "node_status" : "IDLE", "lock_time" : lock_time, "start_time": get_time(), "pwd" : os.getcwd(), "version" : __version__, "task" : {}}
            self._redis.config_set("notify-keyspace-events", "KEA")
            self._watcher = self._redis.pubsub()
            self._watcher.subscribe(["__keyspace@0__:task_pending", f"__keyspace@0__:to_{get_node_name()}"])
            self._resource_cond = resource
            os.system("tmux rename-window cam%d"%os.getpid())
            self._update_node_status()
        except Exception as e:
            print(e)
            traceback.print_exc()
            log_warn(f"Cannot connect to the server {self._conf['server']}:{self._conf['port']}")
            exit(-1)
        while True:
            msg = self._get_message()
            if self._node_status['node_status'] == "RUNNING":
                if not self.p.is_alive():
                    self._handle_task_finished()
            if msg is None:
                continue
            log_info(msg)
            if msg['type'] == 'RUN' and self._node_status['node_status'] != "RUNNING":
                task = msg['task']
                self._log_queue[task['task_id']] = multiprocessing.Queue()
                task['cmd'] = prefix + task['cmd'] + suffix
                self.p = multiprocessing.Process(target = run_cmd, args = (task['cmd'], self._log_queue[task['task_id']], get_node_name(), self._conf))
                self.p.start()
                task['start_time'] = get_time()
                task['node'] = get_node_name()
                task['host'] = get_host_name()
                task['status'] = "RUNNING"
                task['pwd'] = os.getcwd()
                self._node_status['node_status'] = "RUNNING"
                self._node_status['task'] = task
                self._redis.hset('task_running', task['task_id'], json.dumps(task))
                self._redis.hset("worker_lock", get_host_name(), get_time())
                self._remove_by_tid("task_pending", task['task_id'])
                self._update_node_status()
                self._log[task['task_id']] = ""
                log_info(f"Start running task {self._node_status['task']['task_id']}.")
            if msg['type'] == 'KILL':
                if msg['task_id'] == self._node_status['task']['task_id']:
                    task = self._redis.hget("task_running", msg['task_id'])
                    if task is None:
                        continue
                    task =  json.loads(task)
                    task['status'] = "KILLED"
                    task['end_time'] = get_time()
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
            elif msg['type'] == 'TASK_FINISHED':
                self._handle_task_finished()


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
            prow['status'] = "CANCELED"
            prow['end_time'] = get_time()
            self._hset("task_finished", prow["task_id"], prow)
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

    def web(self, host = "127.0.0.1", port = 8257, rebuild = False):
        """
        Open a web panel.
        Run `cam web --rebuild True` to rebuild the page.
        """
        log_info("Nodejs is required to run the web page.")
        log_info("It is strongly suggested to run only on your local machine. NEVER run this on a public server!")
        root = os.path.dirname(os.path.abspath(__file__))
        if not os.path.exists(f"{root}/web/node_modules") or rebuild:
            os.system(f"cd {root}/web && npm install")
            os.system(f"cd {root}/web && npm run build")
        os.system(f"cd {root}/web && HOST={host} PORT={port}  npm run start")


def main():
    Cam = CAM()
    fire.Fire(Cam)

if __name__ == '__main__':
    main()