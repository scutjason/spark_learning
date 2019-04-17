package learn.wordCountCluster;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by minfin data on 2019-04-16
 */
public class WordCount {

    // 本地spark测试程序，提交到集群之前，现在本地测试一下
    public static void main(String[] args) {
//        String[] jars = {"build/libs/scala-0.1.jar"};
        SparkConf conf = new SparkConf().
                setAppName("WordCountCluster");

        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取hdfs文件，读取之前要先上传spark.txt，hdfs fs -put spar.txt /spark.txt
        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/spark.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> paris = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = paris.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + ": " + wordCount._2);
            }
        });

        sc.close();
    }
}
