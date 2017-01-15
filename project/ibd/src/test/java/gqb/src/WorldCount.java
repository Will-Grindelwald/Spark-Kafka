package gqb.src;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

public class WorldCount {

	public static void main(String[] args) {

		// 1.构建 Streaming Context 对象
		SparkConf conf = new SparkConf().setMaster("mesos://192.168.125.171:8081").setAppName("WorldCount")
				.set("spark.executor.uri", "/home/null");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

		// 2.创建 InputDStream,这里选择的是套接字作为数据源，其他的数据源可以是kafka,flume,file等
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9998);

		// 3.操作DStream
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		});
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// Print the first ten elements of each RDD generated in this DStream to
		// the console
		wordCounts.print();
		
		//4.启动Spark Streaming
		jssc.start(); // Start the computation
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
