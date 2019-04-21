package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-21
  */
object WordCountPlusScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountPlusScala").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/spark.txt")
    val pairs = lines.flatMap(r=>r.split(" "))
    val wordsRdd = pairs.map(r=>(r,1))
    val wordCountRdd = wordsRdd.reduceByKey((x,y)=>x+y)
    val countWordRdd = wordCountRdd.map(r=>(r._2,r._1))
    val sortedCountWordRdd = countWordRdd.sortByKey(false)
    val sortedWordCountRdd = sortedCountWordRdd.map(r=>(r._2,r._1))
    sortedWordCountRdd.foreach(r=>println(r._1+": "+r._2))
    sc.stop()
  }
}