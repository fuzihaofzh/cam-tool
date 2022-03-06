#!/usr/bin/env python
import fire
import os
import yaml
import redis
import json
import time
import socket
import logging
from tabulate import tabulate
from pathlib import Path
from subprocess import Popen
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
    return socket.gethostname().split('.', 1)[0]

def table_list(data, headers = None):
    return tabulate([json.loads(d.decode("utf-8")) for d in data], headers = headers)

def _log(info, color):
    csi = '\033['
    colors = {
    "red" : csi + '31m',
    "green" : csi + '32m',
    "yellow" : csi + '33m',
    "blue" : csi + '34m'
    }
    end = csi + '0m'
    print("{0}[CAM]{1} ".format(colors[color], end), info)

def log_info(*args):
    _log("".join(args), "blue")

def log_warn(*args):
    _log("".join(args), "red")

class CAM(object):
    def __init__(self):
        if not os.path.exists(CONFIG_FILE):
            open(CONFIG_FILE, "w").write(DEFAULT_CONF)
        self.conf = yaml.load(open(CONFIG_FILE).read(), yaml.FullLoader) 
        self.redis = redis.StrictRedis(host=self.conf["server"], port=self.conf["port"], password=self.conf["password"], db=0) 

    def __del__(self):
        if hasattr(self, "running_job_id"):
            self.remove_by_tid("running", self.running_job_id)
            self.p.kill()

    def remove_by_tid(self, part, tid):
        part_str = self.redis.lrange(part, 0, -1)
        pending = [json.loads(d.decode("utf-8")) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                self.redis.lrem(part, 1, part_str[i])
                return part_str[i]

    def get_by_tid(self, part, tid):
        part_str = self.redis.lrange(part, 0, -1)
        pending = [json.loads(d.decode("utf-8")) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                return pending[i]
        return None

    def set_by_tid(self, part, tid, lst):
        part_str = self.redis.lrange(part, 0, -1)
        pending = [json.loads(d.decode("utf-8")) for d in part_str]
        for i in range(len(pending)):
            if pending[i][0] == tid:
                self.redis.lset(part, i, lst)

    def server(self, port = None):
        port = self.conf["port"] if port is None else port
        os.system("redis-server --port {0} --requirepass {1}".format(port, self.conf["password"]))

    def client(self):
        while True:
            cnt = self.redis.llen("pending")
            if cnt > 0:
                row_str = self.redis.rpop("pending")
                self.running_job_id, ptime, cmd, status = json.loads(row_str.decode("utf-8"))
                self.redis.lpush("running", json.dumps([self.running_job_id, get_time(), cmd, get_host()]))
                log_info("Running command: ", cmd)
                #os.system(cmd)
                self.p = Popen(cmd, shell=True)
                while True:
                    try:
                        self.p.wait(timeout = 10)
                        break
                    except:
                        if self.get_by_tid("running", self.running_job_id)[-1] == "KILLED":
                            self.p.kill()
                            log_warn("Task ", self.running_job_id, " has been killed.")
                log_info("Finished command: ", cmd)
                self.remove_by_tid("running", self.running_job_id)
            time.sleep(5)
            
    def add(self, cmd, order = -1):
        cnt = self.redis.get('jobid')
        cnt = 0 if cnt is None else int(cnt.decode("utf-8"))
        if order == -1:
            self.redis.lpush("pending", json.dumps([cnt, get_time(), cmd, "Pending"]))
        else:
            self.redis.rpush("pending", json.dumps([cnt, get_time(), cmd, "Pending"]))
        self.redis.set('jobid', cnt + 1)

    def ls(self):
        log_info("Server: ", self.conf['server'])
        pending = self.redis.lrange("pending", 0, -1)
        running = self.redis.lrange("running", 0, -1)
        print(table_list(pending + running, headers = ["ID", "Time", "Command", "Host"]))

    def config(self):
        os.system("vim {0}".format(CONFIG_FILE))

    def kill(self, rid):
        prow = self.get_by_tid(rid, "pending")
        if prow is not None:
            log_warn("The task will be removed: \n", self.remove_by_tid("pending", rid))
        rrow = self.get_by_tid("running", rid)
        if rrow is not None:
            rrow[-1] = "KILLED"
            self.set_by_tid("running", rid, json.dumps(rrow))

def main():
    Cam = CAM()
    fire.Fire(Cam)

if __name__ == '__main__':
    main()