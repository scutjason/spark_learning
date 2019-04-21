package learn;

import com.fasterxml.jackson.annotation.JacksonAnnotationValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by scutjason on 2019-04-18
 */
public class TransformationExample {

    // 将集合中的每个元素乘以2
    public static void doubleNumbers(JavaSparkContext sc){
        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        JavaRDD<Integer> doubleNumbers = numberRDD.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1*2;
            }
        });
        doubleNumbers.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    public static void filterNumbers(JavaSparkContext sc){
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Boolean call(Integer v1) throws Exception {
                // 为true就保留，false就过滤掉
                return v1 % 2 == 0;
            }
        });
        evenNumberRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    // 将文本行拆成多个单词
    public static void flatMapNumbers(JavaSparkContext sc){
        List<String> lines = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> linesRDD = sc.parallelize(lines);
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        wordsRDD.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    //每个班级按照成绩分组 groupByKey
    public static void groupByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 75),
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 65));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(scores);
        // groupByKey 返回的是Iterable
        JavaPairRDD<String, Iterable<Integer>> scoreRDD = pairRDD.groupByKey();
        scoreRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println("class: "+stringIterableTuple2._1);
                Iterator<Integer> iterator = stringIterableTuple2._2.iterator();
                while (iterator.hasNext()){
                    System.out.println(iterator.next());
                }
                System.out.println("**********************");
            }
        });
    }

    // 统计每个班级的总分
    public static void reduceByKey(JavaSparkContext sc){
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 75),
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 65));
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(scores);
        // 第一个和第二个Integer, Integer代表第一个和第二个key的元素类型
        // 第三个Integer代表reduce操作的call方法返回的值得类型。
        JavaPairRDD<String, Integer> scoreRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        scoreRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("class: "+ stringIntegerTuple2._1 + ", scores: "+stringIntegerTuple2._2);
            }
        });

    }

    // 按照学生的成绩排序

    /**
     * 我们用java8的lambda表达式来写foreach
     * @param sc
     */
    public static void sortByKey(JavaSparkContext sc){
        List<Tuple2<Integer, String>> scores = Arrays.asList(
                new Tuple2<>(80, "leo"),
                new Tuple2<>(55, "tom"),
                new Tuple2<>(90, "mary"),
                new Tuple2<>(70, "jack"));
        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(scores);

        // sortByKey
        // sortByKey() 默认是升序排序
        // sortByKey(false) 降序排序
        // 降序升序不改变本身rdd的内容，只是顺序不同了
        JavaPairRDD<Integer, String> sortedSoresRDD = pairRDD.sortByKey(false);

        sortedSoresRDD.foreach((sortedSoresRdd)->System.out.println("student: "+sortedSoresRdd._2 + ", scores " +sortedSoresRdd._1));
    }

    // 打印学生成绩
    public static void joinAndCogroup(JavaSparkContext sc){
        List<Tuple2<Integer, String>> students = Arrays.asList(
                new Tuple2<>(1, "leo"),
                new Tuple2<>(2, "tom"),
                new Tuple2<>(3, "mary"),
                new Tuple2<>(4, "jack"));
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<>(1, 75),
                new Tuple2<>(2, 52),
                new Tuple2<>(3, 92),
                new Tuple2<>(4, 86));

        JavaPairRDD<Integer, String> studentsRDD = sc.parallelizePairs(students);
        JavaPairRDD<Integer, Integer> scoresRDD = sc.parallelizePairs(scores);
        // join可以将两个RDD连接起来，按照key。
        // join之后的key还是两个rdd共同的key，value是原始RDD的value组合而来。
        // 哪个在前，它的value就在前。比如student.join(score)
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScoresRDD = studentsRDD.join(scoresRDD);
        studentScoresRDD.foreach((studentScoresRdd) -> {
            System.out.println("student id: "+studentScoresRdd._1);
            System.out.println("student name: "+studentScoresRdd._2._1);
            System.out.println("student score: "+studentScoresRdd._2._2);
            System.out.println("************************");
        });

        // cogroup
        // 相当于同一个key join上的所有value都放到一个iterable上了。
        // join是tuper2列表，cogroup是一个tuple2，但是里面的元素是iterable
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = studentsRDD.cogroup(scoresRDD);
        cogroupRDD.foreach(cogroupRdd -> {
            System.out.println("student id: " + cogroupRdd._1);
            System.out.println("student name: "+cogroupRdd._2._1);
            System.out.println("student score: "+cogroupRdd._2._2);
            System.out.println("*************************");
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TransformationExample");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        doubleNumbers(sc);
//        filterNumbers(sc);
//        flatMapNumbers(sc);
//        groupByKey(sc);
//        reduceByKey(sc);
//        sortByKey(sc);
        joinAndCogroup(sc);
        sc.stop();
    }
}
