package learn.wordCountLocal;

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
        // SparkConf的一些配置, setMaster 设置spark master
        // 节点的url，如果是local就表示本地执行
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");

        // sparkContext 上下文，初始化
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建一个RDD，按行读取文件
        JavaRDD<String> lines = sc.textFile("src/main/resources/spark.txt");

        // 定义RDD的一些计算操作，如果操作简单就直接定义匿名内部类，如果function很复杂就定义一个单独类, 继承FlatMapFunction
        // 读取每一行，按空格分割成词列表
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            // 序列化操作，保证经过网络传输或者对象存储时保证是同一个对象。
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        // 接下来将每个单词映射成(word, 1)形式
        JavaPairRDD<String, Integer> paris = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        // 然后将后面tuper[1] 的词频相加即可，reduce操作
        JavaPairRDD<String, Integer> wordCounts = paris.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 最后一步，action操作，前面都是transformation操作，map、reduce，
        // action 一般是foreach，可以理解为拿结果
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }
        });

        sc.close();
    }
}
