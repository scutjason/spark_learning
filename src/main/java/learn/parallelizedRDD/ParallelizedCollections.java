package learn.parallelizedRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by scutjason on 2019-04-18
 */
public class ParallelizedCollections {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().
                setAppName("ParallelizedCollections").
                setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建list
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        // 创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers, 5);

        // reduce算子
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        sc.stop();

        System.out.println("sum 1-10: " + sum);
    }
}
