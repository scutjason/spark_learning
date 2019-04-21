package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Created by scutjason on 2019-04-20
 */
public class PersistRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PersistRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long stime = System.currentTimeMillis();
        JavaRDD<String> lines = sc.textFile("src/main/resources/large_text.txt").cache();
//        lines.cache();
//        lines.persist(StorageLevel.MEMORY_AND_DISK());
//        lines.unpersist();
        lines.count();
        System.out.println("first line_count " + (System.currentTimeMillis() - stime));

        stime = System.currentTimeMillis();
        lines.count();
        System.out.println("second line_count " + (System.currentTimeMillis() - stime));

        sc.stop();
    }
}
