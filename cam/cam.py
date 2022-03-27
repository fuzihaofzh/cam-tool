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
HOME = str(Path.home())
CONFIG_FILE = "{0}/.cam.conf".format(HOME)
DEFAULT_CONF="""server: 127.0.0.1
port: 3857
password: 0a8148539c426d7c008433172230b551
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
    print("{0}[CAM {1}] {2} ".format(colors[color], get_time(), end), info)

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
    return json.loads(data.decode("utf-8"))

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
        proc = Popen(cmd, shell=True, stdout = PIPE, stderr=PIPE, bufsize=0)
        for content in proc.stdout:
            content = content.decode("utf-8")
            print(content, end = "")
            log_queue.put(content)
    except Exception as e:
        return 


class UserStdout(object):
    def __init__(self):
        self.terminal = sys.stdout
        self._log = ""

    def write(self, message):
        self.terminal.write(message)
        self._log += message

class CAM(object):
    def __init__(self):
        self.__version__ = __version__
        if not os.path.exists(CONFIG_FILE):
            open(CONFIG_FILE, "w").write(DEFAULT_CONF)
        self._conf = yaml.load(open(CONFIG_FILE).read(), yaml.FullLoader) 
        self._redis = redis.StrictRedis(host=self._conf["server"], port=self._conf["port"], password=self._conf["password"], db=0, encoding="utf-8")
        self._channels = {}
        self._status = {}
        self._log_queue = multiprocessing.Queue()
        self._log = ""
        self._server_fails = False

    def __del__(self):
        kill_subs()  

    def _log_sys_info(self):
        log_info("Server: ", self._conf['server'], ":", str(self._conf['port']), ' v', self.__version__)           

    def _condition_parse(self, cond):
        #e.g.:
        #Has Free GPU   : "bash('nvidia-smi').count(' 0MiB /') > 2"
        #Slurm job count: "int(bash('squeue -h -t pending,running -r | wc -l')) < 4"
        #Slurm node count: "bash('squeue').count('ltl-gpu')<4"
        if cond == "":
            return True
        else:
            return eval(cond)

    def _publish(self, channel, msg):
        try:
            self._redis.publish(channel, json.dumps(msg))
            if self._server_fails:
                self._log_sys_info()
                self._server_fails = False
        except Exception as e:
            if not self._server_fails:
                log_warn(e)
                self._server_fails = True

    def _get_message(self, channel):
        try:
            if channel not in self._channels or not self._channels[channel].subscribed:
                self._channels[channel] = self._redis.pubsub()
                self._channels[channel].subscribe(channel)
            msg = self._channels[channel].get_message()
            msg = msg['data'] if msg is not None and 'data' in msg and type(msg['data']) is bytes else None
            if self._server_fails:
                self._log_sys_info()
                self._server_fails = False
            return json.loads(msg) if msg is not None else None
        except Exception as e:
            if not self._server_fails:
                log_warn(e)
                self._server_fails = True

    def _make_worker_msg(self, **kwargs):
        msg = {"type" : "STATUS", "node" : get_node_name(), "host" : get_host_name(), "time": get_time(), "pwd" : os.getcwd()}
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
        for node in self.node_list:
            if get_seconds_passed(self.node_list[node]['time']) > 60:
                del self.node_list[node]
        for task_id, task in self.task_pending.items():
            if 'assigned_time' in task:
                continue
            for nid, node in self.node_list.items():
                #print(task_id, task, nid, node)
                if task['host'] is None or node['host'] in task['host']:
                    msg = self._make_server_msg(type = "RUN", cmd = task['cmd'], task_id = task_id)
                    self._publish("to_%s"%nid, msg)
                    task['assigned_time'] = get_time()
                    log_info("Assign task %d to node %s."%(task_id, nid))
                    log_info(str(msg))
        for lst in [self.task_pending, self.task_running]:
            for task_id, task in lst.items():
                if task['status'] == 'KILLED':
                    self.task_finished[task_id] = task
                    del lst[task_id]

    def _server_handle_message(self, msg):
        if msg is None:
            return
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
        elif msg['type'] == 'WORKER_TASK_KILL':
            if msg['task_id'] in self.task_running:
                self.task_running[msg['task_id']] = 'KILLED'
        elif msg['type'] == 'STDOUT':
            if msg['task_id'] in self.task_running:
                log_info("Get stdout of task %d"%msg['task_id'])
                self._publish("to_%s"%self.task_running[msg['task_id']]['node'], self._make_server_msg(type = "STDOUT"))
                self.ask_node[self.task_running[msg['task_id']]['node']] = msg['node']
        elif msg['type'] == 'STDOUT_RES':
            log_info("Receive stdout of task %s"%msg['node'])
            self._publish("to_%s"%self.ask_node[msg['node']], self._make_server_msg(type = "STDOUT_RES", stdout_res = msg['stdout_res']))
        elif msg['type'] == 'GET_STATUS':
            self._publish("to_%s"%msg['node'], self._make_server_msg(type = "SERVER_STATUS", status = {"running" : dict(self.task_running), "pending" : dict(self.task_pending), "finished" : dict(self.task_finished), "nodes" : self.node_list}))
        elif msg['type'] == 'STATUS':
            self.node_list[msg['node']] = msg
            if msg['status'] == 'RUNNING':
                if msg['task_id'] in self.task_pending:
                    self.task_running[msg['task_id']] = msg
                    del self.task_pending[msg['task_id']]
            if msg['status'] == 'FINISHED':
                if msg['task_id'] in self.task_running:
                    self.task_finished[msg['task_id']] = self.task_running[msg['task_id']]
                    self.task_finished[msg['task_id']]['finish_time'] = msg['time']
                    del self.task_running[msg['task_id']]
                    log_info("Task %d finished."%(msg['task_id']))
                    log_info(str(msg))
            elif msg['status'] == 'IDLE':
                #self._publish("to_%s"%msg['node'], self._make_server_msg(type = "RUN", cmd = "./test.sh", task_id = 0))
                pass
        
    def server(self, port = None):
        """
        Start the server.
        """
        port = self._conf["port"] if port is None else port
        cmd = "redis-server --port {0} --requirepass {1}".format(port, self._conf["password"])
        redis_p = Popen(cmd, shell=True)
        self.task_pending = collections.OrderedDict()
        self.task_running = collections.OrderedDict()
        self.task_finished = collections.OrderedDict()
        self.node_list = collections.OrderedDict()
        self.ask_node = {}
        self.task_cnt = 0
        self._log_sys_info()
        while True:
            time.sleep(0.1)
            self._server_handle_tick()
            msg = self._get_message("to_server")
            self._server_handle_message(msg)
            
            
    def worker(self):
        self._log_sys_info()
        self._status['status'] = "IDLE"
        #self.userout = UserStdout()
        #sys.stdout = self.userout
        #sys.stderr = self.userout
        while True:
            time.sleep(0.1)  
            self._publish("to_server", self._make_worker_msg())
            if self._status['status'] == "RUNNING":
                if not self.p.is_alive():
                    self._status['end_time'] = get_time()
                    self._status['status'] = "FINISHED"
            msg = self._get_message("to_%s"%get_node_name())
            if msg is not None:
                log_info(msg)
            if msg is None:
                continue
            elif msg['type'] == "RUN" and self._status['status'] != "RUNNING":
                #self.p = Popen(msg['cmd'], shell=True, stdout = PIPE, stderr=PIPE, bufsize=0)
                self.p = multiprocessing.Process(target = run_cmd, args = (msg['cmd'], self._log_queue))
                self.p.start()
                self._status['status'] = "RUNNING"
                self._status['cmd'] = msg['cmd']
                self._status['start_time'] = get_time()
                self._status['task_id'] = msg['task_id']
                self._status['node'] = get_node_name()
            elif msg['type'] == "KILL":
                if self._status['task_id'] == msg['task_id']:
                    log_info("Killing running task %d ." % self._status['task_id'])
                    self._status['status'] = "KILLING"
                    self._publish("to_server", self._make_worker_msg())
                    kill_subs()
                    log_warn("Task ", str(msg['task_id']), " has been killed.")
                    self._publish("to_server", self._make_worker_msg(status = 'WORKER_TASK_KILLED', task_id = self._status['task_id']))
                    self._status = {"status" : "IDLE"}
            elif msg['type'] == "REPORT":
                res = eval(msg['cmd'])
                self._publish("to_server", self._make_worker_msg(type = 'REPORT', res = res))
            elif msg['type'] == "STDOUT":
                while not self._log_queue.empty():
                    txt = self._log_queue.get_nowait()
                    self._log += txt
                self._publish("to_server", self._make_worker_msg(type = 'STDOUT_RES', stdout_res = self._log))

    def add(self, cmd):
        self._publish("to_server", self._make_worker_msg(type = 'ADD', cmd = cmd, host = None))

    def kill(self, task_id):
        self._publish("to_server", self._make_worker_msg(type = 'KILL', task_id = task_id, host = None))

    def log(self, tid):
        self._publish("to_server", self._make_worker_msg(type = 'STDOUT', task_id = tid, host = None))
        msg = self._get_message("to_%s"%get_node_name())
        while msg is None:
            time.sleep(1)
            msg = self._get_message("to_%s"%get_node_name())
        print(msg['stdout_res'])

    def ls(self):
        self._publish("to_server", self._make_worker_msg(type = 'GET_STATUS', host = None))
        msg = self._get_message("to_%s"%get_node_name())
        while msg is None:
            time.sleep(1)
            msg = self._get_message("to_%s"%get_node_name())
        for k in msg['status']:
            print(k)
            print(msg['status'][k])
        

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