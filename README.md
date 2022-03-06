# Cloud Assignment Manager Tool
CAM-Tool is a cloud assignment manager tool that helps you to manage your tasks across different machines. You can start several workers across different machines and upload the command to cam-tool. Cam-tool will then distribute the tasks to workers automatically.

## Install
```
pip install cam-tool
conda install redis # only required for server machine.
```

## Config
The config file is located at `~/.cam.conf`. You can set the server address, port, and password for redis. You can simply run `cam config` to edit the yaml file.

## Start Server
On the server machine, simply run the following command to start the server.
```
cam server
```

## Start Worker
On a worker machine, please run the following command to start a worker. You can start many worker on the same machine.
```
cam worker
``` 

## Add new task
Please run the following command to add a new task
```
cam add "ls -lah"
```

## Status
You can see the status of eash task with the `ls` command:
```
> cam ls

ID  Time                 Command    Host
----  -------------------  ---------  -------
   3  2022-03-07 06:39:33  ls -lah    Pending
```

## Kill tasks
You can kill task with its task id.
```
cam kill 3
```
