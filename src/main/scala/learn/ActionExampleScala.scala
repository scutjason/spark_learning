package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-19
  */
object ActionExampleScala {

  def reduce(sc: SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRdd = sc.parallelize(numbers)
    val reduceRdd = numberRdd.reduce(_+_)
    println(reduceRdd)
  }

  def collect(sc: SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRdd = sc.parallelize(numbers)
    val doubleNumberRdd = numberRdd.map(_*2)
    val collectRdd = doubleNumberRdd.collect()
    for(ele <- collectRdd){
      println(ele)
    }
  }

  def count(sc: SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRdd = sc.parallelize(numbers)
    val countRdd = numberRdd.count()
    println(countRdd)
  }

  def take(sc: SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRdd = sc.parallelize(numbers)
    val doubleNumberRdd = numberRdd.map(_*2)
    val takeRdd = doubleNumberRdd.take(3)
    for(ele <- takeRdd){
      println(ele)
    }
  }

  def savaAsTextFile(sc: SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRdd = sc.parallelize(numbers)
    val doubleNumberRdd = numberRdd.map(_*2)
    doubleNumberRdd.saveAsTextFile("src/main/resources/doubleNumber.txt")
  }

  def countByKey(sc: SparkContext): Unit ={
    val scores = Array(("class1", 89), ("class2", 90), ("class1", 72), ("class2", 67))
    val scoresRDD = sc.parallelize(scores, 1)
    val classCounts = scoresRDD.countByKey()
    for(sn <- classCounts){
      println("class name: "+sn._1 + ", students: "+sn._2)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ActionExampleScala")
    val sc = new SparkContext(conf)
//    reduce(sc)
//    collect(sc)
//    count(sc)
//    savaAsTextFile(sc)
    countByKey(sc)
    sc.stop()

  }
}