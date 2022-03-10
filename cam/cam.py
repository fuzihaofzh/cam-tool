#!/usr/bin/env python
from cgitb import reset
from urllib.parse import parse_qsl
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
from tabulate import tabulate
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

def get_host():
    return socket.gethostname().split('.', 1)[0] + " " + str(os.getpid())

def time_diff(now, st):
    return str(now - st).split('.')[0].replace(' day, ', '-')

def table_list(data, headers = None):
    return tabulate(data, headers = headers, tablefmt="plain")

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
    for child in parent.children(recursive=True):
        for i in range(3):
            child.send_signal(signal.SIGINT)
            time.sleep(0.3)

class CAM(object):
    def __init__(self):
        self.__version__ = __version__
        if not os.path.exists(CONFIG_FILE):
            open(CONFIG_FILE, "w").write(DEFAULT_CONF)
        self._conf = yaml.load(open(CONFIG_FILE).read(), yaml.FullLoader) 
        self._redis = redis.StrictRedis(host=self._conf["server"], port=self._conf["port"], password=self._conf["password"], db=0)

    def __del__(self):
        if hasattr(self, "worker_id"):
            try:
                self._redis.hdel("workers", self.worker_id)
            except:
                pass
        if hasattr(self, "running_job_id"):
            try:
                self.kill(self.running_job_id)
                self._store_finished_task()
                #os.killpg(os.getpgid(self.p.pid), signal.SIGKILL)
                kill_subs()
                #self.p.kill()
            except:
                pass
        

    def _remove_by_tid(self, part, tid):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [parse_json(d) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                self._redis.lrem(part, 1, part_str[i])
                return part_str[i].decode("utf-8")

    def _get_by_tid(self, part, tid):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [parse_json(d) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                return pending[i]
        return None

    def _set_by_tid(self, part, tid, lst):
        part_str = self._redis.lrange(part, 0, -1)
        pending = [parse_json(d) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                self._redis.lset(part, i, lst)
                return
        self._redis.lpush(part, lst)
        

    def _condition_parse(self, cond):
        #e.g.:
        #Has Free GPU   : "bash('nvidia-smi').count(' 0MiB /') > 2"
        #Slurm job count: "int(bash('squeue -h -t pending,running -r | wc -l')) < 4"
        #Slurm node count: "bash('squeue').count('ltl-gpu')<4"
        if cond == "":
            return True
        else:
            return eval(cond)

    def _store_finished_task(self):
        data = self._get_by_tid("running", self.running_job_id)
        st = datetime.datetime.fromisoformat(data[1])
        now = datetime.datetime.utcnow()
        peroid = time_diff(now, st)
        data[1] = peroid
        data[-1] = ("Finished " if not data[-1].startswith("KILLED") else "") + data[-1]
        data.append(get_time())
        self._set_by_tid("finished", self.running_job_id, json.dumps(data))
        self._remove_by_tid("running", self.running_job_id)
        

    def server(self, port = None):
        """
        Start the server.
        """
        log_info("Server: ", self._conf['server'], ":", str(self._conf['port']), ' v', self.__version__)
        port = self._conf["port"] if port is None else port
        os.system("redis-server --port {0} --requirepass {1}".format(port, self._conf["password"]))

    def worker(self, cond = "", cmdprefix = "", cmdsuffix = ""):
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
        log_info("Worker {0} started.".format(get_host()))
        worker_start_time = get_time()
        self.worker_id = get_host()
        os.system("tmux rename-window cam%d"%os.getpid())
        while True:
            try:
                cnt = self._redis.llen("pending")
                if not hasattr(self, "server_disconnected") or self.server_disconnected:
                    self.server_disconnected = False
                    log_info("Server Connected.")
                    log_info(" ".join(["Server:", self._conf['server']+":"+str(self._conf['port']), ' v'+self.__version__, cond, cmdprefix, cmdsuffix]))
                if not self._condition_parse(cond):
                    self._redis.hset("workers", self.worker_id, json.dumps([worker_start_time, "Wait Resource", cond, cmdprefix, cmdsuffix]))                
                elif cnt <= 0:
                    self._redis.hset("workers", self.worker_id, json.dumps([worker_start_time, "Wait Task", cond, cmdprefix, cmdsuffix]))
                else:
                    row_str = self._redis.rpop("pending")
                    if row_str is None:
                        continue
                    self.running_job_id, ptime, cmd, status = parse_json(row_str)
                    cmd = "".join([cmdprefix, cmd, cmdsuffix])
                    self._redis.lpush("running", json.dumps([self.running_job_id, get_time(), cmd, get_host()]))
                    log_info("{0} Running task: {1}".format(self.worker_id, self.running_job_id))
                    log_info("{0} Running command: {1}".format(self.worker_id, cmd), )
                    self.p = Popen(cmd, shell=True)#preexec_fn=os.setsid
                    worker_start_time = get_time()
                    while True:
                        try:
                            self._redis.hset("workers", self.worker_id, json.dumps([worker_start_time, "Running %d"%self.running_job_id, cond, cmdprefix, cmdsuffix]))
                            tf = self._get_by_tid("running", self.running_job_id)
                            taskinfo = taskinfo if tf is None else json.dumps(tf) 
                            self._set_by_tid("running", self.running_job_id, taskinfo)
                            if self._get_by_tid("running", self.running_job_id)[-1].startswith("KILLED"):
                                kill_subs()
                                #parent.kill()
                                log_warn("Task ", str(self.running_job_id), " has been killed.")
                                break
                        except Exception as e:
                            log_warn("ERROR:")
                            print(e)
                            self.server_disconnected = True
                            time.sleep(10)
                        try:
                            self.p.wait(timeout = 10)
                            break
                        except:
                            pass
                    log_info("{0} Finished command: {1}".format(self.worker_id, cmd))
                    self._store_finished_task()
                    delattr(self, "server_disconnected")
            except Exception as e:
                log_warn("ERROR:")
                print(e)
                self.server_disconnected = True
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
        log_info("New Task: ", str(cnt))
        self._redis.set('jobid', cnt + 1)

    def ls(self, type = None, maxwidth = None):
        """
        Show the status of all tasks.
        <br>`cam ls` will list both tasks and workers information.
        <br>`cam ls worker 30` will list all workers wile each column has at most 30 chars.
        <br>`cam ls task 30` will list all tasks wile each column has at most 30 chars.
        <br>`cam ls finished` will list all finished tasks.
        """
        now = datetime.datetime.utcnow()
        log_info("Server: ", self._conf['server'], ":", str(self._conf['port']), ' v', self.__version__)
        def get_lst(part):
            return sorted([parse_json(d) for d in self._redis.lrange(part, 0, -1)], key=lambda x: -x[0])
        try:
            if type is None or type == "task":
                pending = get_lst("pending")
                running = get_lst("running")
                finished = get_lst("finished")[:3]
                res = pending + running + finished
                nres = []
                for i in range(len(res)):
                    data = res[i]
                    if i < len(pending + running):
                        st = datetime.datetime.fromisoformat(data[1])
                        data[1] = time_diff(now, st)
                    if maxwidth is not None:
                        data[2] = data[2][:maxwidth]
                    nres.append(data)
                print(table_list(nres, headers = ["ID", "Time", "Command", "Host/PID"]))
                print("Pending: ", len(pending), " Running: ", len(running), " Finished: ", len(finished))
            if type is None or type == "worker":
                workers = self._redis.hgetall("workers")
                info = []
                for w in workers:
                    dt = parse_json(workers[w])
                    st = datetime.datetime.fromisoformat(dt[0])
                    lst = [w, time_diff(now, st)] + dt[1:]
                    if maxwidth is not None and len(lst) > 3:
                        lst[3] = lst[3][:maxwidth]
                    info.append(lst)
                info = sorted(info, key=lambda x: x[0])
                print(table_list(info, headers = ["Worker/PID", "Up Time", "Status", "cond", "prefix", "suffix"]))
                status = {"Wait Resource": 0, "Wait Task": 0, "Running": 0}
                for ir in info:
                    if ir[2].startswith("Running"):
                        status["Running"] += 1
                    else:
                        status[ir[2]] += 1
                for k in status:
                    print("{0} : {1}; ".format(k, status[k]), end="")
            if type == "finished":
                nres = get_lst("finished")[::-1]
                print(table_list(nres, headers = ["ID", "Time", "Command", "Host/PID", "FnishTime"]))
                print("Total: ", len(nres))  
        except:
            log_warn("Server Disconnected.")

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
            rrow[-1] = "KILLED " + rrow[-1]
            self._set_by_tid("running", rid, json.dumps(rrow))

    def refresh(self, type = None):
        """
        Refresh worker list. e.g. 
        <br>`cam refresh`
        """
        if type is None or type == "worker":
            self._redis.delete("workers")
        if type is None or type == "task":
            self._redis.delete("running")

def main():
    Cam = CAM()
    #fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.core.Display = lambda lines, out: print(*[l.replace('<br>', '\n\t') for l in lines], file=out)
    fire.Fire(Cam)

if __name__ == '__main__':
    main()