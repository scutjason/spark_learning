package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-21
  */
object SecondarySortByKeyScala {
  class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable{

    override def >(that: SecondarySortKey): Boolean = super.>(that)

    override def >=(that: SecondarySortKey): Boolean = super.>=(that)

    override def <(that: SecondarySortKey): Boolean = super.<(that)

    override def <=(that: SecondarySortKey): Boolean = super.<=(that)

    override def equals(obj: scala.Any): Boolean = super.equals(obj)

    override def compare(that: SecondarySortKey): Int = {
      if (this.first - that.first != 0){
        this.first - that.first
      }else{
        this.second - that.second
      }
    }

    override def compareTo(that: SecondarySortKey): Int = super.compareTo(that)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondarySortByKeyScala").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/sort.txt")
    val pairs = lines.map(r=>{
      val lineSplit = r.split(" ")
      val tuple2 = Tuple2[SecondarySortKey, String](new SecondarySortKey(lineSplit(0).toInt, lineSplit(1).toInt), r)
      tuple2
    })
    val sortPairs = pairs.sortByKey()
    sortPairs.foreach(r=>println(r._2))

  }
}