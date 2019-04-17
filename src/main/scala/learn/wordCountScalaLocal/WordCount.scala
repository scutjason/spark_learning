package learn.wordCountScalaLocal

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountScalaLocal").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/spark.txt")
    val words = lines.flatMap(line => line.split(" "))   // lines.flatMap(_.split(" "))
    val paris = words.map(word => (word, 1))  // words.map((_, 1))
    val wordCount = paris.reduceByKey(_ + _)

    wordCount.foreach(wordCount => println(wordCount._1 + ": " + wordCount._2))

    sc.stop()
  }
}
