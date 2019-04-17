package learn.actors


//因为需要走网络，所以此trait需要继承Serializable
trait RemoteMessage extends Serializable {}

//worker------>master
case class RegisterWorker(var workerId: String, var cores: Int, var memory: Int) extends RemoteMessage

//master ---->worker
case class RegisteredWorker() extends RemoteMessage

//worker ------> worker
object SendHeartBeat

//worker------>master
case class SendHeartBeat(var workerId: String)

//master ------> master
object CheckTimeOutWorker

