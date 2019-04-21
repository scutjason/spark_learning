package learn.ParallizedCollectionsScala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-18
  */
object ParallelizedCollections {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizedCollections").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers, 5); // 5是指定的partition
    val sum = numbers.reduce(_+_);
    println("sum 1-10: "+sum)
  }
}