package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-21
  */
object GroupTop3Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("GroupTop3Scala")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/score.txt")
    val pairs = lines.map(r=>(r.split(" ")(0), r.split(" ")(1)))
    val groupPairs = pairs.groupByKey()
    val top3GroupPairs = groupPairs.map(r=>(r._1, r._2.toList.sortWith(_>_).take(3)))
    top3GroupPairs.foreach(r=>{
      println("classname: "+r._1)
      r._2.foreach(x=>println(x))
    })
    sc.stop()
  }
}
