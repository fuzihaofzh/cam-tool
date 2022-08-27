<template>
<div>
<nobr>
<el-input
  type="textarea"
  :autosize="{ maxRows: 1}"
  resize="none"
  @keydown.native="input_keydown"
  style="width: 70%;"
  v-model="cmd">
</el-input>
<el-select v-model="sel_host" placeholder="All Nodes">
    <el-option
      v-for="item in hosts"
      :key="item"
      :label="item"
      :value="item"
      >
    </el-option>
  </el-select>
</nobr>
<el-tabs @tab-click="fetch_status" value="nodes">
    <el-tab-pane :label="pendingStr" name="pending">
      <el-table :data="pendingTable" style="width: 100%">
        <el-table-column prop="task_id" label="Pending">
          <template slot-scope="table">
            <span v-html="table.row.task_id" :class="'bold'"/>
            <span v-html="table.row.status" :class="status_color[table.row.status]"/>
             <el-button type="danger" icon="el-icon-close" size="mini" @click="kill_task(table.row.task_id, table.row.host)"></el-button>
            <span v-html="table.row.cmd+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.host"/>
            <br>
            <span v-html="table.row.submit_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.duration"/>
          </template>
        </el-table-column>
      </el-table>
    </el-tab-pane>
    <el-tab-pane :label="runningStr" name="running">
      <el-table :data="runningTable" style="width: 100%" @expand-change="on_expand_change">
        <el-table-column type="expand">
          <template slot-scope="table">
          <el-input
            :class="'elinput-task'+table.row.task_id"
            type="textarea"
            :autosize="{ maxRows: 20}"
            :readonly="true"
            v-model="log_info[table.row.task_id]">
          </el-input>
          </template>
        </el-table-column>
        <el-table-column prop="task_id" label="Running">
          <template slot-scope="table">
            <span v-html="table.row.task_id" :class="'bold'"/>
            <span v-html="table.row.status" :class="status_color[table.row.status]"/>
            <el-button type="danger" icon="el-icon-close" size="mini" @click="kill_task(table.row.task_id)"></el-button>
            <span v-html="table.row.cmd"/>
            <br>
            <span v-html="table.row.node+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.pwd+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.submit_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.start_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.duration"/>
          </template>
        </el-table-column>
      </el-table>
    </el-tab-pane>
    <el-tab-pane :label="finishedStr" name="finished">
      <el-table :data="finishedTable" style="width: 100%" @expand-change="on_expand_change">
        <el-table-column type="expand">
          <template slot-scope="table">
          <el-input
            :class="'elinput-task'+table.row.task_id"
            type="textarea"
            :autosize="{ maxRows: 20}"
            :readonly="true"
            v-model="log_info[table.row.task_id]">
          </el-input>
          </template>
        </el-table-column>
        <el-table-column prop="task_id" label="Finished">
          <template slot-scope="table">
            <span v-html="table.row.task_id" :class="'bold'"/>
            <span v-html="table.row.status" :class="status_color[table.row.status]"/>
            <span v-html="table.row.cmd"/>
            <br>
            <span v-html="table.row.node+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.pwd+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.submit_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.start_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.end_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.duration"/>
          </template>
        </el-table-column>
      </el-table>
    </el-tab-pane>
    <el-tab-pane :label="nodesStr" name="nodes">
      <el-table :data="nodesTable" style="width: 100%" @expand-change="on_expand_change">
        <el-table-column type="expand">
          <template slot-scope="table">
          <el-input
            :class="'elinput-task'+table.row.task.task_id"
            type="textarea"
            :autosize="{ maxRows: 20}"
            :readonly="true"
            v-model="log_info[table.row.task.task_id]">
          </el-input>
          </template>
        </el-table-column>
        <el-table-column prop="task_id" label="Nodes">
          <template slot-scope="table">
            <span v-html="table.row.node" :class="'bold'"/>
            <span v-html="table.row.node_status" :class="status_color[table.row.node_status]"/>
            <span v-html="'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.prefix+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.priority+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.resource+'&nbsp;&nbsp;&nbsp;&nbsp;v'+table.row.version"/>
            <br>
            <span v-html="table.row.task.task_id+'&nbsp;&nbsp;&nbsp;&nbsp;' + table.row.task.cmd +'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.pwd+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.start_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.task.start_time+'&nbsp;&nbsp;&nbsp;&nbsp;'+table.row.duration"/>
          </template>
        </el-table-column>
      </el-table>
    </el-tab-pane>
  </el-tabs>
</div>
</template>


<script>
import moment from 'moment'
async function fetch_config(){
  const conf = await fetch('/get_config');
  return conf;
}


function get_table(data){
  var dt = []
  for(var k in data ){
    dt.push(JSON.parse(data[k]))
    var ki = dt.length - 1
    if('start_time' in dt[ki] || 'submit_time' in dt[ki]){
      var start_time = 'start_time' in dt[ki] ? (new Date(dt[ki]['start_time'] + 'Z')) : (new Date(dt[ki]['submit_time'] + 'Z'));
      var end_time = 'end_time' in dt[ki] ? (new Date(dt[ki]['end_time'] + 'Z')) : (new Date());
      var ms = end_time - start_time;
      var d = moment.duration(ms);
      var s = Math.floor(d.asHours()) + moment.utc(ms).format(":mm:ss");
      dt[ki]['duration'] = s;
    }
  }
  if(dt.length > 0 && 'end_time' in dt[0]){
    dt.sort((a, b) => (a['end_time'] > b['end_time']) ? 1 : -1)
  }
  if(dt.length > 0 && 'resource' in dt[0]){
    dt.sort((a, b) => {
      if (a['resource'] != b['resource']) return ((a['resource'] > b['resource']) ? 1 : -1)
      if (a['node'] != b['node']) return ((a['node'] > b['node']) ? 1 : -1)
      })
  }
  return dt.reverse();
}


export default {
  name: 'IndexPage',
  data(){
    return {
      finished_task_columns: ["task_id", "cmd",  "host", "node", "pwd", "start_time", "finish_time", "status", "time", "type"],
      finishedTable: [],
      pendingTable: [],
      runningTable: [],
      nodesTable: [],
      pendingStr: "Pending",
      runningStr: "Running",
      finishedStr: "Finished",
      nodesStr: "Nodes",
      status_color: {
        "RUNNING"       : "font_green",
        "PENDING"       : "font_blue",
        "FINISHED"      : "font_cyan",
        "KILLED"        : "font_red",
        "CANCELED"      : "font_red",
        "IDLE"          : "font_blue",
        "DISCONNECTED"  : "font_red",
        "WAIT LOCK"     : "font_yellow",
        "WAIT RESOURCE" : "font_yellow",
      },
      lock: false,
      log_info: {},
      interval_id : {},
      task_node : {},
      cmd: "",
      history: [],
      history_id: null,
      hosts: [""],
      sel_host: ""
    }
  },
  async mounted () {
    //setInterval(this.update_info, 1000);
    var conf = await fetch('/get_config').then(res => res.json());
    try {
      const ws = new WebSocket(`ws://${conf['user_config']['webserver']}:${conf['user_config']['port']+1}/`);
      ws.onmessage = ({data}) => {
        this.message =  data;
        //console.log(this.message);
        this.fetch_status();
      }
    } catch(err) {
      console.log(err);
    }
    await this.fetch_status()
    for (var task of this.finishedTable.reverse().concat(this.runningTable.reverse()).concat(this.pendingTable.reverse())){
      var prefix = "";
      if (this.nodesDict[task['node']] != null){
        prefix = this.nodesDict[task['node']]['prefix'];
      }
      this.history.push(task['cmd'].replace(prefix, ""))
    }
  },
  methods: {
    async fetch_status(){
      console.log("in fetch_status")
      for(var tid in this.interval_id){
        clearInterval(this.interval_id[tid])
      }
      this.interval_id = {}
      var status = await fetch('/redis?type=status').then(res => res.json());
      this.pendingTable = get_table(status['pending'])
      this.pendingStr = `Pending(${this.pendingTable.length})`
      this.runningTable = get_table(status['running'])
      this.runningStr = `Running(${this.runningTable.length})`
      this.finishedTable = get_table(status['finished'])
      this.finishedStr = `Finished(${this.finishedTable.length})`
      this.nodesTable = get_table(status['nodes'])
      this.nodesDict = Object.assign({}, ...this.nodesTable.map((x) => ({[x.node]: x})));
      this.nodesStr = `Nodes(${this.nodesTable.length})`
      var hosts = new Set()
      for (var node of this.nodesTable){
        hosts.add(node['host']);
      }
      this.hosts = [...hosts]
    },
    async kill_task(task_id, host = ""){
      await fetch(`/redis?type=kill&tid=${task_id}&host=${host}`);
    },
    async on_expand_change(row, expandedRows){
      if ('task' in row){
        var tid = row['task']['task_id']
        var node = row['task']['node']
      }else{
        var tid = row['task_id']
        var node = row['node']
      }
      console.log("tid:", tid)
      this.update_task_log(tid, node)
      var boxs = document.getElementsByClassName('elinput-task'+tid)
        for(var box of boxs){
          box.firstChild.scrollTop = box.firstChild.scrollHeight;
        }
      if(this.task_in(tid, this.runningTable) && !(tid in this.interval_id)){
        this.interval_id[tid] = setInterval(this.update_task_log, 1000, tid, node);
      }
      if (expandedRows.length == 0 && tid in this.interval_id){
        clearInterval(this.interval_id[tid])
        delete this.interval_id[tid]
      }
    },
    async update_task_log(tid, node){
      var res = await fetch(`/redis?type=stdout&tid=${tid}&node=${node}`).then(res => res.json())
      res['stdout'] = res['stdout'] == null ? "" : res['stdout']
      await setTimeout(function() {}, 800);
      if (this.log_info[tid] != res['stdout']){
        this.$set(this.log_info, tid, res['stdout'])
        var boxs = document.getElementsByClassName('elinput-task'+tid)
        for(var box of boxs){
          box.firstChild.scrollTop = box.firstChild.scrollHeight;
        }
      }
      if (this.task_in(tid, this.finishedTable) && tid in this.interval_id){
        clearInterval(this.interval_id[tid])
        delete this.interval_id[tid]
      }
    },
    update_info(){
      for(var task in this.taskLogStatus){
        if (this.taskLogStatus[task] == true || this.log_info[task] == null){
          this.update_task_log(task, this.task_node[task])
        }
      }
    },
    task_in(tid, lst){
      for(var item of lst){
        if(item['task_id'] == tid) return true
      }
      return false
    },
    async input_keydown(event){
      if(event.key == 'Enter'){
        event.preventDefault()
        this.cmd = this.cmd.replace('\t', '')
        var res = fetch(`/redis?type=run&cmd=${encodeURI(this.cmd)}&host=${this.sel_host}`);
        if(this.cmd.length > 0) this.history.push(this.cmd);
        this.cmd = ""
        if(this.sel_host != ""){
          await setTimeout(function(){},1000);
          this.fetch_status()
        }
      }else if(event.key == 'ArrowUp'){
        this.history_id = this.history_id == null ? this.history.length - 1 : this.history_id - 1;
        this.history_id = (this.history_id < 0 ? this.history_id + this.history.length : this.history_id);
        this.cmd = this.history[(this.history.length + this.history_id) % this.history.length]
      }else if(event.key == 'ArrowDown'){
        this.history_id = this.history_id == null ? this.history.length : this.history_id + 1;
        this.cmd = this.history[(this.history.length + this.history_id) % this.history.length]
      }else{
        this.history_id = null;
      }
    }
  }
}
</script>

<style scoped>
.align_right { float:right; }
.font_red {color: #dc322f;}
.font_blue {color: #268bd2;}
.font_green {color: #859900;}
.font_cyan {color: #2aa198;}
.font_yellow {color: #b58900;}
.bold {font-weight: bold;}
</style>
