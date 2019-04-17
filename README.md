# spark_learn
spark learning

## spark集群提交
```
1、gradle build
```

```
2、scp -r scala-1.0-SNAPSHOT.jar root@spark1:/usr/local/learn_spark
```

```
3、spark-submit \
--class learn.wordCountCluster.WordCount \
--master spark://spark1:7077  \
--num-executors 3 \
--executor-memory 1g  --driver-memory 1g  \
/usr/local/learn_spark/wordCount/scala-1.0-SNAPSHOT.jar
```

```
4、http://spark1:8080/
```
### 问题记录

1、scala编译版本和spark集群版本不一致， 集群上 /usr/local/spark/jars/scala-compiler-2.11.12.jar，本地用的2.12.6
注意是改 compile group: 'org.apache.spark', name: 'spark-core_2.11', version: '2.4.1'
```
Exception in thread "main" java.lang.BootstrapMethodError: java.lang.NoClassDefFoundError: scala/runtime/java8/JFunction2$mcIII$sp
	at learn.wordCountScalaCluster.WordCount$.main(WordCount.scala:15)
	at learn.wordCountScalaCluster.WordCount.main(WordCount.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:849)
	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:167)
	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:195)
```

