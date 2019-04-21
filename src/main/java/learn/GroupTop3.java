package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import sun.rmi.server.InactiveGroupException;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by scutjason on 2019-04-21
 *
 * 取出每个班级的前三名
 */
public class GroupTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("GroupTop3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("src/main/resources/score.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(r->{
            return new Tuple2(r.split(" ")[0], Integer.valueOf(r.split(" ")[1]));
        });

        JavaPairRDD<String, Iterable<Integer>> groupPairs = pairs.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> top3ClassScores = groupPairs.mapToPair(r->{
            Integer[] top3 = new Integer[3];
            Iterator<Integer> iters = r._2.iterator();
            while(iters.hasNext()){
                int now = iters.next();
                // 外部值遍历一次，内部的top再按大小排序
                for (int i = 0; i< 3; i++){
                    if(top3[i] == null){
                        top3[i] = now;
                        break;
                    }else if (top3[i] < now){
                        int tmp = top3[i];
                        top3[i] = now;
                        now = tmp;
                    }
                    System.out.println(r._1+": "+top3[0]+", "+top3[1]+", "+top3[2]);
                }
            }
            return new Tuple2<>(r._1, Arrays.asList(top3));
        });
        top3ClassScores.foreach(r->{
            System.out.println("classname: "+r._1);
            for (int i: r._2){
                System.out.println(i);
            }
            System.out.println("************************");
        });
        sc.stop();
    }
}
