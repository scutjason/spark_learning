package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by scutjason on 2019-04-18
 */
public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().
                setAppName("LineCountLocal").
                setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("src/main/resources/test_line_count.txt");
        JavaPairRDD<String, Integer> paris = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> lineCount = paris.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        lineCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("line: " + stringIntegerTuple2._1 + ", times: "+stringIntegerTuple2._2);
            }
        });
    }

}
