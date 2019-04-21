package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-18
  */
object LineCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("lineCountScala").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/test_line_count.txt")
    val paris = lines.map(line => (line, 1))
    val lineCount = paris.reduceByKey(_ + _)
    lineCount.foreach(lineCount => println(lineCount._1 + ": times " + lineCount._2))
  }
}