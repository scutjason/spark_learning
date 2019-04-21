package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;
import scala.math.Ordered;
import sun.rmi.server.InactiveGroupException;

/**
 * Created by scutjason on 2019-04-21
 */

// 首先要实现一个自定义排序的key，继承scala的Ordered 和 Serializable序列化
public class SecondarySortByKey {

    static class MySecondarySort implements Ordered<MySecondarySort>, Serializable {
        private static final long serialVersionUID = 1L;

        // 首先定义一个排序的key
        private int first;
        private int second;

        public MySecondarySort(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean $greater(MySecondarySort other) {
            // 判断谁大
            if (this.first > other.getFirst()) {
                return true;
            } else if (this.first == other.getFirst() &&
                    this.second > other.getSecond()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean $greater$eq(MySecondarySort other) {
            // 大于等于
            if (this.$greater(other)) {
                return true;
            } else if (this.first == other.getFirst() &&
                    this.second >= other.getSecond()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean $less(MySecondarySort other) {
            // 小于
            if (this.first < other.getFirst()) {
                return true;
            } else if (this.first == other.getFirst() &&
                    this.second < other.getSecond()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean $less$eq(MySecondarySort other) {
            // 小于等于
            if (this.$less(other)) {
                return true;
            } else if (this.first == other.getFirst() &&
                    this.second <= other.getSecond()) {
                return true;
            } else {
                return false;
            }
        }


        @Override
        public int compare(MySecondarySort other) {
            // 两个对象比较
            if (this.first - other.getFirst() != 0) {
                // 第一个key不相等，就比较diyigekey
                return this.first - other.getFirst();
            } else {
                // 否则比较第二个key
                return this.second - other.getSecond();
            }
        }

        // sortByKey
        // 调用 compareTo方法
        // 负数表示小于，0表示等于，正数表示大于
        @Override
        public int compareTo(MySecondarySort other) {
            // 两个对象比较
            if (this.first - other.getFirst() != 0) {
                // 第一个key不相等，就比较diyigekey
                return this.first - other.getFirst();
            } else {
                // 否则比较第二个key
                return this.second - other.getSecond();
            }
        }

        // 为自定义的key实现getter和setter
        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public int getSecond() {
            return second;
        }

        public void setSecond(int second) {
            this.second = second;
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySortByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/sort.txt");
        JavaPairRDD<MySecondarySort, String> pairs = lines.mapToPair(r->{
            String[] lineSplit = r.split(" ");
            MySecondarySort mySecondarySort = new MySecondarySort(
                    Integer.valueOf(lineSplit[0]), Integer.valueOf(lineSplit[1]));
            return new Tuple2<>(mySecondarySort, r);
        });

        JavaPairRDD<MySecondarySort, String> sortedPairs = pairs.sortByKey();
        JavaRDD<String> sortedLines = sortedPairs.map(r->r._2);
        sortedLines.foreach(r-> System.out.println(r));

        sc.stop();
    }
}
