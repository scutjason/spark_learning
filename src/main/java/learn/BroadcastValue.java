package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;

import java.util.Arrays;
import java.util.List;

/**
 * Created by scutjason on 2019-04-20
 */
public class BroadcastValue {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastValue").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int factor = 3;
        // 定义广播变量
        final Broadcast<Integer> broadcastValue = sc.broadcast(factor);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        // 取值用value()
        JavaRDD<Integer> tripleNumbersRDD = numbersRDD.map(r -> r*broadcastValue.value());
        tripleNumbersRDD.foreach(r -> System.out.println(r));

        sc.stop();
    }
}

