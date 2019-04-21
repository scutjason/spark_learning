package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-20
  */
object BroadcastValueScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastValueScala").setMaster("local")
    val sc = new SparkContext(conf)
    val factor = 3
    val bc_factor = sc.broadcast(factor)
    val numbers = Array(1,2,3,4,5)
    val numbersRdd = sc.parallelize(numbers, 1)
    val tripleNumber = numbersRdd.map(_*bc_factor.value)
    tripleNumber.foreach(r => println(r))

    sc.stop()
  }
}