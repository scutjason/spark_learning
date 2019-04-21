package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import sun.rmi.server.InactiveGroupException;

import java.util.Arrays;

/**
 * Created by scutjason on 2019-04-20
 *
 * word count 升级版
 * 1、统计每个单词出现的频率  reduceByKey
 * 2、按照单词出现的频率降序排列 sortByKey
 */

public class WordCountPlus {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCountPlus");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/spark.txt");
        // flatMap返回的是迭代器
        JavaRDD<String> wordsRDD = lines.flatMap(r-> Arrays.asList(r.split(" ")).iterator());
        JavaPairRDD<String, Integer> paris = wordsRDD.mapToPair(r->new Tuple2<>(r,1));
        JavaPairRDD<String, Integer> wordCountRDD = paris.reduceByKey((x,y)->x+y);
        // 把(hello, 1) 映射成 (1, hello)
        JavaPairRDD<Integer, String> countWordRDD = wordCountRDD.mapToPair(r->new Tuple2<>(r._2, r._1));
        JavaPairRDD<Integer, String> sortedCountWordRDD = countWordRDD.sortByKey(false);
        JavaPairRDD<String, Integer> sortedWordCountRDD = sortedCountWordRDD.mapToPair(r->new Tuple2<>(r._2, r._1));
        sortedWordCountRDD.foreach(r-> System.out.println(r._1+": "+r._2));
        sc.stop();
    }
}
