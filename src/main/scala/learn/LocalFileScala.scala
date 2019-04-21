package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-18
  */
object LocalFileScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/spark.txt")
    val line_count = lines.map(_.length)
    // 第一个_ 表示第一个元素，第二个_表示不同与第一个的元素。
    val file_count =line_count.reduce(_+_)
    sc.stop()
    println(file_count)
  }
}
