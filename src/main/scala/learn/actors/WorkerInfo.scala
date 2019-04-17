package learn.actors

class WorkerInfo (var workerId: String, var cores: Int, var memory: Int) {
  /**
    * 用来封装Worker的基本信息
    */
  //worker最近汇报心跳的时间
  var lastHeartBeatTime:Long = System.currentTimeMillis()
}
