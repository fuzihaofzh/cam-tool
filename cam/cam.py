#!/usr/bin/env python
from urllib.parse import parse_qsl
import fire
import os
import yaml
import redis
import json
import time
import socket
import subprocess
from tabulate import tabulate
from pathlib import Path
from subprocess import Popen, PIPE
import datetime
HOME = str(Path.home())
CONFIG_FILE = "{0}/.cam.conf".format(HOME)
DEFAULT_CONF="""server: 127.0.0.1
port: 3857
password: 0a8148539c426d7c008433172230b551
"""

def get_time():
    return str(datetime.datetime.now()).split('.')[0]

def get_host():
    return socket.gethostname().split('.', 1)[0] + " " + str(os.getpid())

def table_list(data, headers = None):
    return tabulate(data, headers = headers)

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
    _log("".join(args), "blue")

def log_warn(*args):
    _log("".join(args), "red")

def bash(cmd):
    return subprocess.getoutput(cmd)

class CAM(object):
    def __init__(self):
        if not os.path.exists(CONFIG_FILE):
            open(CONFIG_FILE, "w").write(DEFAULT_CONF)
        self._conf = yaml.load(open(CONFIG_FILE).read(), yaml.FullLoader) 
        self._redis = redis.StrictRedis(host=self._conf["server"], port=self._conf["port"], password=self._conf["password"], db=0)

    def __del__(self):
        if hasattr(self, "running_job_id"):
            self._remove_by_tid("running", self.running_job_id)
            self.p.kill()
        if hasattr(self, "worker_id"):
            self._redis.hdel("workers", self.worker_id)

    def _remove_by_tid(self, part, tid):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [json.loads(d.decode("utf-8")) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                self._redis.lrem(part, 1, part_str[i])
                return part_str[i].decode("utf-8")

    def _get_by_tid(self, part, tid):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [json.loads(d.decode("utf-8")) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                return pending[i]
        return None

    def _set_by_tid(self, part, tid, lst):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [json.loads(d.decode("utf-8")) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                self._redis.lset(part, i, lst)

    def _condition_parse(self, cond):
        #e.g.:
        #Has Free GPU   : "bash('nvidia-smi').count(' 0MiB /') > 2"
        #Slurm job count: "int(bash('squeue -h -t pending,running -r | wc -l')) < 4"
        #Slurm node count: "bash('squeue').count('ltl-gpu')<4"
        if cond == "":
            return True
        else:
            return eval(cond)
        

    def server(self, port = None):
        """
        Start the server.
        """
        port = self._conf["port"] if port is None else port
        os.system("redis-server --port {0} --requirepass {1}".format(port, self._conf["password"]))

    def worker(self, cond = "", cmdprefix = "", cmdsuffix = ""):
        """
         Start the worker. 
        <br>`cam worker "some start condition"`
        <br>Start condition can be specified with bash and python e.g.: 
        <br>Has Free GPU\t: "bash('nvidia-smi').count(' 0MiB /') > 2"
        <br>Slurm job count\t: "int(bash('squeue -h -t pending,running -r | wc -l')) < 4"
        <br>Slurm node count\t: "bash('squeue').count('ltl-gpu')<4"
        """
        log_info("Worker started. Listening to {0} ...".format(self._conf['server']))
        worker_start_time = get_time()
        self.worker_id = get_host()
        os.system("tmux rename-window cam%d"%os.getpid())
        self._redis.hset("workers", self.worker_id, json.dumps([worker_start_time, "Wait Task"]))
        while True:
            cnt = self._redis.llen("pending")
            if cnt <= 0:
                self._redis.hset("workers", self.worker_id, json.dumps([worker_start_time, "Wait Task", cond, cmdprefix, cmdsuffix]))
            elif not self._condition_parse(cond):
                self._redis.hset("workers", self.worker_id, json.dumps([worker_start_time, "Wait Resource", cond, cmdprefix, cmdsuffix]))
            else:
                row_str = self._redis.rpop("pending")
                if row_str is None:
                    continue
                self.running_job_id, ptime, cmd, status = json.loads(row_str.decode("utf-8"))
                cmd = "".join([cmdprefix, cmd, cmdsuffix])
                self._redis.lpush("running", json.dumps([self.running_job_id, get_time(), cmd, get_host()]))
                log_info("Running command: ", cmd)
                self.p = Popen(cmd, shell=True)
                self._redis.hset("workers", self.worker_id, json.dumps([worker_start_time, "Running %d"%self.running_job_id, cond, cmdprefix, cmdsuffix]))
                while True:
                    try:
                        if self._get_by_tid("running", self.running_job_id)[-1] == "KILLED":
                            self.p.kill()
                            log_warn("Task ", self.running_job_id, " has been killed.")
                            break
                    except:
                        if not hasattr(self, "server_disconnected"):
                            log_warn("Server Disconnected.")
                            self.server_disconnected = True
                    try:
                        self.p.wait(timeout = 10)
                        break
                    except:
                        pass
                log_info("Finished command: ", cmd)
                self._remove_by_tid("running", self.running_job_id)
            time.sleep(5)
            
    def add(self, cmd, order = -1):
        """
        Add a new task.
        """
        cnt = self._redis.get('jobid')
        cnt = 0 if cnt is None else int(cnt.decode("utf-8"))
        if order == -1:
            self._redis.lpush("pending", json.dumps([cnt, get_time(), cmd, "Pending"]))
        else:
            self._redis.rpush("pending", json.dumps([cnt, get_time(), cmd, "Pending"]))
        self._redis.set('jobid', cnt + 1)

    def ls(self, type = None):
        """
        Show the status of all tasks.
        """
        now = datetime.datetime.now()
        if type is None or type == "task":
            log_info("Server: ", self._conf['server'])
            pending = self._redis.lrange("pending", 0, -1)
            running = self._redis.lrange("running", 0, -1)
            res = pending + running
            nres = []
            for i in range(len(res)):
                data = json.loads(res[i].decode("utf-8"))
                st = datetime.datetime.fromisoformat(data[1])
                data[1] = str(now - st).split('.')[0]
                nres.append(data)
            print(table_list(nres, headers = ["ID", "Time", "Command", "Host/PID"]))
            print()
        if type is None or type == "worker":
            workers = self._redis.hgetall("workers")
            info = []
            for w in workers:
                dt = json.loads(workers[w].decode("utf-8"))
                st = datetime.datetime.fromisoformat(dt[0])
                lst = [w, str(now - st).split('.')[0]] + dt[1:]
                info.append(lst)    
            print(table_list(info, headers = ["Worker/PID", "Up Time", "Status", "cond", "prefix", "suffix"]))

    def config(self):
        """
        Edit the config file ~/.cam.conf
        """
        os.system("vim {0}".format(CONFIG_FILE))

    def kill(self, rid):
        """
        kill task by its id. e.g. 
        <br>`cam kill 2`
        """
        prow = self._get_by_tid("pending", rid)
        if prow is not None:
            log_warn("The task will be removed: \n", self._remove_by_tid("pending", rid))
        rrow = self._get_by_tid("running", rid)
        if rrow is not None:
            rrow[-1] = "KILLED"
            self._set_by_tid("running", rid, json.dumps(rrow))

def main():
    Cam = CAM()
    #fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.core.Display = lambda lines, out: print(*[l.replace('<br>', '\n\t') for l in lines], file=out)
    fire.Fire(Cam)

if __name__ == '__main__':
    main()