package com.ljc.spark.streaming.example;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 *   `$ bin/spark-submit --class com.ljc.sparkStreaming.JavaKafkaWordCount --master yarn --deploy-mode cluster \
 *          ibd-0.0.1-jar-with-dependencies.jar zoo01,zoo02,zoo03 my-consumer-group ljc_topic1,ljc_topic2 2`
 */

public class JavaKafkaWordCount {

	// 为方便调试, 这几个参数写死了
	private static final String ZKQUORUM = "192.168.125.171:2181,192.168.125.172:2181,192.168.125.173:2181";
	private static final String GROUP = "ljc_group";

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: JavaKafkaWordCount <topics> <numThreads>");
			System.exit(1);
		}

		// 1. 构建 Streaming Context 对象, 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

		// 2. 从 kafka 消息源获取数据, 直接将 value 当作行, 不管 key
		int numThreads = Integer.parseInt(args[1]);
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = args[0].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, ZKQUORUM, GROUP, topicMap);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 6971311137558483044L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		// 3. 分割出单词
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 8007852536409493432L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(SPACE.split(x)).iterator();
			}
		});

		// 4. 批内单词计数
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = -7890759411539780573L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = -2457563665581562711L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// 5. Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();

		// 6. 启动 Spark Streaming
		jssc.start();
		jssc.awaitTermination();
	}
}
