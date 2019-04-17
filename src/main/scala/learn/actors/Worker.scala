package learn.actors

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class Worker (var cores: Int, var memory: Int) extends Actor {
  /**
    * 向master进行注册，加入到集群中去；
    * 定时向master汇报心跳。
    */
  var masterActor: ActorSelection = null
  var workerId = UUID.randomUUID().toString
  val heartBeatInterval = 3000

  //actor启动后会立即执行此方法，只会执行一次
  override def preStart(): Unit = {
    // 获取master的Actor
    masterActor = context.actorSelection(
      "akka.tcp://masterActorSystem@localhost:9416/user/masterActor")
    //2、worker启动后立即向master进行注册
    masterActor ! RegisterWorker(workerId, cores, memory)
  }

  //用来接收消息的方法，这个方法会被多次执行，只要有消息过来就会被执行
  override def receive: Receive = {
    case "started" => println("worker setup sucessful.....")

    //master发送过来的注册成功的消息
    case RegisteredWorker => {
      //导入定时器
      import context.dispatcher //开启定时任务
      //此处的millis需要导入import scala.concurrent.duration._
      context.system.scheduler.schedule(0 millis, heartBeatInterval millis, self, SendHeartBeat)
    }

    //向master汇报心跳
    case SendHeartBeat => { masterActor ! SendHeartBeat(workerId) }
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    val workerHost = "localhost"
    val workerPort = "8416"
    /**
      * 在实际应用场景下，有时候我们就是确实需要在scala创建多少字符串，但是每一行需要固定对齐。
      * 解决该问题的方法就是应用scala的stripMargin方法，在scala中stripMargin默认是“|”作为出来连接符，
      * 在多行换行的行头前面加一个“|”符号即可。
      * 当然stripMargin方法也可以自己指定“定界符”,同时更有趣的是利用stripMargin.replaceAll方法，
      * 还可以将多行字符串”合并”一行显示。
      */
    val configStr =
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname="$workerHost"
         |akka.remote.netty.tcp.port="$workerPort"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    //创建线程池对象ActorSystem
    val workerActorSystem = ActorSystem("workerActorSystem", config)

    //使用ActorSystem创建actor
    //创建一个Worker对象，Cores为62，memory为128
    val workerActor = workerActorSystem.actorOf(Props(new Worker(62, 128)), "workerActor")

    //给新创建的masterActor发送一条消息，发送消息使用感叹号"!"

    workerActor ! "started"
    //将ActorSystem阻塞在这，不要让其停止

    workerActorSystem.whenTerminated
  }
}

