package learn.wordCountScalaCluster

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-17
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountScalaCluster")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark1:9000/spark.txt")
    val words = lines.flatMap(_.split(" "))
    val paris = words.map((_,1))
    val wordCount = paris.reduceByKey(_+_)
    wordCount.foreach(wordCount => println(wordCount._1 + ": " + wordCount._2))

    sc.stop()
  }
}