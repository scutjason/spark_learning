package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-18
  */
object TransformationExampleScala {

  // 2倍list
  def douberNumbers(sc: SparkContext): Unit = {
    val numbers = Array(1,2,3,4,5)
    val numberRDD = sc.parallelize(numbers)
    val doubleNumber = numberRDD.map(_*2)
    doubleNumber.foreach(doubleNumber => println(doubleNumber))
  }

  // 保留偶数
  def filterNumbers(sc: SparkContext): Unit = {
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRdd = sc.parallelize(numbers)
    val filter = numberRdd.filter(_ % 2 == 0)
    filter.foreach(filter => println(filter))
  }

  // 将文本拆分成单词
  def flatMapLines(sc: SparkContext): Unit ={
    val lines = Array("hello you", "hello me", "hello world")
    val linesRDD = sc.parallelize(lines);
    val wordsRDD = linesRDD.flatMap(_.split(" "))
    wordsRDD.foreach(wordsRDD => println(wordsRDD))
  }

  // 按照班级组合成绩
  def groupByKey(sc: SparkContext): Unit ={
    val scores = Array(("class1", 89), ("class2", 90), ("class1", 72), ("class2", 67))
    val scoresRDD = sc.parallelize(scores, 1)
    val classScoresRDD = scoresRDD.groupByKey()
    classScoresRDD.foreach(score => {
      println(score._1)
      score._2.foreach(sigleScore => println(sigleScore))
      println("*****************")
    })
  }

  def reduceByKey(sc: SparkContext): Unit ={
    val scores = Array(("class1", 89), ("class2", 90), ("class1", 72), ("class2", 67))
    val scoresRDD = sc.parallelize(scores, 1)
    val classScoresRDD = scoresRDD.reduceByKey(_+_)
    classScoresRDD.foreach(classScoresRDD =>
      println("class: " + classScoresRDD._1 + ", scores: " +classScoresRDD._2)
    )
  }

  def sortByKey(sc: SparkContext): Unit ={
    val scores = Array((80, "leo"), (55, "tom"), (90, "mary"), (70, "jack"))
    val scoresRDD = sc.parallelize(scores, 1)
    val sortedScoresRDD = scoresRDD.sortByKey()
    sortedScoresRDD.foreach(sortedScoresRdd =>
      println("student: "+sortedScoresRdd._2 + ", scores " +sortedScoresRdd._1))
  }


  def joinAndCogroup(sc: SparkContext): Unit ={
    val students = Array((1, "leo"), (2, "tom"), (3, "mary"), (4, "jack"))
    val scores = Array((1, 75), (2, 52), (3, 92), (4, 86))
    val studentsRDD = sc.parallelize(students, 1)
    val scoresRDD = sc.parallelize(scores, 1)
    // join
    val studentScoreJoinRDD = studentsRDD.join(scoresRDD)
    studentScoreJoinRDD.foreach(r => {
      println("student id: "+r._1)
      println("student name: "+r._2._1)
      println("student score: "+r._2._2)
      println("************************")
    })

    // cogroup
    //　分组cogroup返回的结构是CompactBuffer，
    // CompactBuffer并不是scala里定义的数据结构，而是spark里的数据结构，
    // 它继承自一个迭代器和序列，所以它的返回值是一个很容易进行循环遍历的集合。
    val studentScoreCogroupRDD = studentsRDD.cogroup(scoresRDD)
    studentScoreCogroupRDD.foreach(r => {
      println("student id: "+r._1)
      println("student name: "+r._2._1)
      println("student score: "+r._2._2)
      println("************************")
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationExampleScala")
    val sc = new SparkContext(conf)
//    douberNumbers(sc)
//    filterNumbers(sc)
//    flatMapLines(sc)
//    groupByKey(sc)
//    reduceByKey(sc)
//    sortByKey(sc)
    joinAndCogroup(sc)
    sc.stop()
  }
}
