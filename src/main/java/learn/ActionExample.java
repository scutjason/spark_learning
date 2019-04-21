package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by scutjason on 2019-04-19
 */

public class ActionExample {
    public static void reduce(JavaSparkContext sc){
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        // 直接用java8的特性: lambda表达式
        int sum = numberRDD.reduce((a,b)->a+b);
        System.out.println(sum);
    }

    // 把远程部署到spark集群的元素，拉到driver本地来
    public static void collect(JavaSparkContext sc){
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> doubleNumbersRDD = numberRDD.map(r->2*r);
        // 这里不用foreach了，在远程集群中变量Map了，
        // 而用collect操作将集群中的Map后的数组拉取到本地，
        // 不建议使用，远程数据量大的时候，拉取到本地耗时又容易oom，
        // 通常还是使用foreach
        for(Integer list :doubleNumbersRDD.collect()){
            System.out.println(list);
        }
    }

    // count，统计元素个数
    // 用得少？
    public static void count(JavaSparkContext sc){
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        System.out.println(numberRDD.count());
    }

    // take， 拿元素
    // 与collect类似，也是从远程集群上拿数据
    // 不同的是take只拿部分
    public static void take(JavaSparkContext sc){
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> doubleNumbersRDD = numberRDD.map(r->2*r);
        for(Integer ele: doubleNumbersRDD.take(3)){
            System.out.println(ele);
        }
    }

    // 将数据保存到文件中，可以是本地文件也可以是hdfs文件
    public static void saveAsTextFile(JavaSparkContext sc1){
        SparkConf conf = new SparkConf().setAppName("saveAsTextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> doubleNumbersRDD = numberRDD.map(r->2*r);
        // 注意保存的是目录，而不是文件。
        doubleNumbersRDD.saveAsTextFile("hdfs://spark1:9000/doubleNumbers.txt");
        sc.stop();
    }

    // 统计每个key对应的value
    public static void countByKey(JavaSparkContext sc){
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 75),
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 65));
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(scores);
        Map<String, Long> studentCount =  pairRDD.countByKey();

        for (String key: studentCount.keySet()) {
            System.out.println("class name: "+key + ", students: "+studentCount.get(key));
        }
    }

    public static void main(String[] args) {
        // 跑saveAsTextFile是要注释sc，因为只允许一个sc。
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExample");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        reduce(sc);
//        collect(sc);
//        count(sc);
//        take(sc);
//        saveAsTextFile(null);
        countByKey(sc);
        sc.stop();
    }
}
