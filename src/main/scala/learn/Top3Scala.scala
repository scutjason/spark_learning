package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-21
  */
object Top3Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3Scala").setMaster("local");
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/top.txt")
    val paris = lines.map(r=>{
      val tuple2 = new Tuple2[Int, String](r.toInt, r)
      tuple2
    })
    val sortedPairs = paris.sortByKey(false);
    val sortedNumber = sortedPairs.map(r=>r._1)
    for (n <- sortedNumber.take(3)) println(n)

    sc.stop()
  }
}