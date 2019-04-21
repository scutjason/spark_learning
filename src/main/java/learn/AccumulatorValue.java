package learn;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import scala.collection.LinearSeq;

import java.util.Arrays;
import java.util.List;

/**
 * Created by scutjason on 2019-04-20
 */
public class AccumulatorValue {

    // 自定义累加变量
    static class MyAccumulator extends AccumulatorV2<Integer, Integer>{
        private Integer num = 0;

        public MyAccumulator() {
        }

        public MyAccumulator(Integer num) {
            this.num = num;
        }

        @Override
        public boolean isZero() {
            return this.num == 0;
        }

        @Override
        public AccumulatorV2<Integer, Integer> copy() {
            return new MyAccumulator(this.num);
        }

        @Override
        public void add(Integer v) {
            this.num += v;
        }

        @Override
        public void reset() {
            this.num = 0;
        }

        @Override
        public void merge(AccumulatorV2<Integer, Integer> other) {
            this.num += other.value();
        }

        @Override
        public Integer value() {
            return this.num;
        }
    }

    // 正常情况
    public static void accumulator_nomal(JavaSparkContext sc){
        Accumulator<Integer> sum = sc.accumulator(0);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        // 用foreach这个action来触发累加变量
        numberRDD.foreach(r->sum.add(r));
        System.out.println(sum.value());
    }

    // 不正确的使用
    public static void accumulator_wrong(JavaSparkContext sc){
        Accumulator<Integer> sum = sc.accumulator(0);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        // 判断偶数的个数，返回到累加器中
        JavaRDD<Integer> evenRDD = numberRDD.map(x->{
            if (x % 2 == 0) {
                sum.add(1);
                // 是偶数就返回1
                return 1;
            }else{
                return 0;
            }
        });
//        evenRDD.count();
        // 将evenRDD持久化。
        evenRDD.cache().count();
        System.out.println(sum.value());
        // 在使用foreach来遍历evenRDD，此时发现sum变成了10
        // 为啥因为每一次action操作会重新执行前面的transformation操作也就是map，
        // 而map中有改变sum的值，也就是说再次进行累加了。
        // 怎么办？
        // 可以把evenRDD 持久化，这样他就不会重新执行了
        evenRDD.foreach(r-> System.out.print(""));
        System.out.println(sum.value());
    }


    public static void main(String[] args) {

        // 可以提issue
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumulatorValue");
        SparkContext sc = new SparkContext(conf);
//        accumulator_nomal(sc);
//        accumulator_wrong(sc);

        // 默认已经regist
        MyAccumulator myAccumulator = new MyAccumulator();
        // 注意这里java的sc没有register接口，必须先用SparkContext先初始化，然后在转成JavaSpark
        sc.register(myAccumulator);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> numberRDD = JavaSparkContext.fromSparkContext(sc).parallelize(numbers);
        // 判断奇数的个数，返回到累加器中
        JavaRDD<Integer> evenRDD = numberRDD.map(x->{
            if (x % 2 == 1) {
                myAccumulator.add(1);
                // 是奇数就返回1
                return 1;
            }else{
                return 0;
            }
        });
        evenRDD.cache().count();
        System.out.println(myAccumulator.value());
        evenRDD.foreach(r-> System.out.print(""));
        System.out.println(myAccumulator.value());
        sc.stop();
    }
}
