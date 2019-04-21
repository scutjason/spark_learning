package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by scutjason on 2019-04-21
 *
 * 取出文本中的前三个数字
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Top3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/top.txt");
        JavaPairRDD<Integer, String> pairs = lines.mapToPair(r->new Tuple2(Integer.valueOf(r), r));
        JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);
        JavaRDD<Integer> sortedNumbers = sortedPairs.map(r->r._1);
        for (Integer i: sortedNumbers.take(3))
            System.out.println(i);
        sc.stop();
    }
}
