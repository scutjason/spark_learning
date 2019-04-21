package learn

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scutjason on 2019-04-20
  */
object AccumulatorValueScala {

  def accumulator_normal(sc: SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd = sc.parallelize(numbers)
    val sum = sc.longAccumulator
    numbersRdd.foreach(r => sum.add(r))
    println(sum.value)
  }

  def accumulator_wrong(sc: SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd = sc.parallelize(numbers)
    val evenCount = sc.longAccumulator
    val evenRDD = numbersRdd.map(r=>{
      if (r % 2 ==0){
        evenCount.add(1)
        1
      }else{
        0
      }
    })
    // 第一次action
//    evenRDD.count()
    evenRDD.cache().count()
    println(evenCount.value)
    // 第二次action
    evenRDD.foreach(r=>print(""))
    println(evenCount.value)
  }

  // 自定义累加变量
  class MyAccumulator extends AccumulatorV2[Int, Int] {
    private[this] var num = 0

    def this(number: Int) = {
      this()
      this.num = number
    }

    override def isZero: Boolean = num == 0

    override def add(v: Int): Unit = num += v

    override def copy(): AccumulatorV2[Int, Int] = new MyAccumulator(num)

    override def merge(other: AccumulatorV2[Int, Int]): Unit = num += other.value

    override def reset(): Unit = num = 0

    override def value: Int = num
  }

  def myAccumulatorTest(sc: SparkContext): Unit ={
    val av = new MyAccumulator
    sc.register(av, "AccumulatorValueScala")

    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd = sc.parallelize(numbers)
    val evenRDD = numbersRdd.map(r=>{
      if (r % 2 ==1){
        av.add(1)
        1
      }else{
        0
      }
    })
    // 第一次action
    //    evenRDD.count()
    evenRDD.cache().count()
    println(av.value)
    // 第二次action
    evenRDD.foreach(r=>print(""))
    println(av.value)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("AccumulatorValueScala")
    val sc = new SparkContext(conf)
//    accumulator_normal(sc);
//    accumulator_wrong(sc)
    myAccumulatorTest(sc)
    sc.stop()
  }
}
