# Cloud Assignment Manager Tool
CAM-Tool is a cloud assignment manager tool that helps you to manage your tasks across different machines. You can start several workers across different machines and upload the command to cam-tool. Cam-tool will then distribute the tasks to workers automatically.

## Install
```
pip install cam-tool
conda install redis # only required for server machine.
conda install -c conda-forge nodejs # only required if you want to use a web-based gui
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
You may also want to add more parameters as resource calculation, prefix or suffix.
```
cam worker "ngpu()" "sbash"
``` 
where `ngpu()` calculate how many GPU is idle and "sbash" is a prefix to all the command running on this server.


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

## Get the log of a worker
You can run the following command to get the log of a running or finished worker:
```
cam log 3
```

## Web Console
Cam tool provides a simple web console to add, kill, and watch tasks. Please install nodejs before you use this function. Just run the following code to start a web server. Never run this command on any public server.
```
cam web
```
Then open the link http://localhost:8257/ and you can see the following user interface:
<img width="1349" alt="gui" src="https://user-images.githubusercontent.com/1419566/161306901-b5e417cb-1f55-4534-b272-636b29a0f754.png">

