package learn.actors

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

class Master extends Actor {
  /**
    *
    * 接收worker的注册，并将worker的注册信息保存下来;
    * 感知worker的上下线;
    * 接收worker的汇报心跳，更新worker的相关信息;
    * 定时检测超时的worker，并将超时的worker从集群中移除掉。
    */
  //worker ---->workInfo
  val idToWorker = new mutable.HashMap[String, WorkerInfo]()

  //WorkInfo
  val workers = new mutable.HashSet[WorkerInfo]()
  val checkTimeOutWorkerInterval = 5000


  //preStart()是Master启动之后立即执行的方法
  override def preStart(): Unit = {
    import context.dispatcher
    //启动一个定时器，定时检查超时的Worker，定时发送CheckTimeOutWorker
    context.system.scheduler.schedule(0 millis, checkTimeOutWorkerInterval millis, self, CheckTimeOutWorker)
  }

  // 重写接受消息的偏函数，其功能是接受消息并处理
  override def receive: Receive = {

    case "started" => println("master startup successful...")

    //接收Worker发送过来的注册信息
    case RegisterWorker(workerId, cores, memory) => {
      if (!idToWorker.contains(workerId)) {
        //3、将发送过来的Worker信息封装成WorkerInfo保存到HashMap和HashSet中
        val workerInfo = new WorkerInfo(workerId, cores, memory)
        idToWorker.put(workerId, workerInfo)
        workers += workerInfo
        //4、master向worker发送注册成功的消息给worker
        //sender指代的是消息源，即谁发送过来的消息，就指代谁
        sender() ! RegisteredWorker
      }
    }

    //Master接收Worker汇报的心跳信息
    case SendHeartBeat(workerId) => {
      //从idToWorker中取出对应的Worker，并更新最近一次汇报心跳的时间
      if (idToWorker.contains(workerId)) {
        val workerInfo = idToWorker(workerId)
        workerInfo.lastHeartBeatTime = System.currentTimeMillis()
        workers += workerInfo
      }
    }

    case CheckTimeOutWorker => {
      val currentTime = System.currentTimeMillis()
      //过滤出超时的Worker（即在指定的时间范围内没有向Master进行汇报）
      val toRemoves = workers.filter(w => currentTime - w.lastHeartBeatTime > checkTimeOutWorkerInterval)
      toRemoves.foreach(worker => {
        idToWorker.remove(worker.workerId)
        workers -= worker}
      )
      println(s"worker count is ${workers.size}")
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    val masterHost = "localhost"
    val masterPort = "9416"
    /**
      * 在实际应用场景下，有时候我们就是确实需要在scala创建多少字符串，但是每一行需要固定对齐。
      * 解决该问题的方法就是应用scala的stripMargin方法，在scala中stripMargin默认是“|”作为出来连接符，
      * 在多行换行的行头前面加一个“|”符号即可。
      * 当然stripMargin方法也可以自己指定“定界符”,同时更有趣的是利用stripMargin.replaceAll方法，
      * 还可以将多行字符串”合并”一行显示。
      */
    // 使用ConfigFactory的parseString方法解析字符串,指定服务端IP和端口
    val configStr =
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname="$masterHost"
         |akka.remote.netty.tcp.port="$masterPort"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    // 创建actor的线程池对象的ActorSystem, 叫 "masterActorSystem"
    val masterActorSystem = ActorSystem("masterActorSystem", config)

    // 使用ActorSystem创建actor，启动
    val masterActor = masterActorSystem.actorOf(Props[Master], "masterActor")

    //给新创建的masterActor发送一条消息，发送消息使用感叹号"!"
    masterActor ! "started"

    //将ActorSystem阻塞在这，不要让其停止
    masterActorSystem.whenTerminated
  }
}
