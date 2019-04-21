package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


/**
 * Created by scutjason on 2019-04-18
 */
public class LocalFile {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().
                setAppName("localfile").
                setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/spark.txt");

        JavaRDD<Integer> lineCount = lines.map(new Function<String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });

        int count_char_file = lineCount.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("文件总的字符数: "+count_char_file);
    }
}
