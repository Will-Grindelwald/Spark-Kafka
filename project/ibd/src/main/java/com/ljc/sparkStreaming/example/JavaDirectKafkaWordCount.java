package com.ljc.sparkStreaming.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/spark-submit --class com.ljc.sparkStreaming.JavaDirectKafkaWordCount --master yarn --deploy-mode cluster \
 *          ibd-0.0.1-jar-with-dependencies.jar broker1-host:port,broker2-host:port topic1,topic2
 */

/**
 * 这个方法有问题, 特别慢, 用另一个
 */

public class JavaDirectKafkaWordCount {

	// 为方便调试, 这几个参数写死了
	private static final String BROKERS = "192.168.125.171:9092,192.168.125.172:9092,192.168.125.173:9092";

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 1) {
			System.err.println("Usage: JavaDirectKafkaWordCount <topics>");
			System.exit(1);
		}

		String brokers = BROKERS;
		String topics = args[0];

		// 1. 构建 Streaming Context 对象, 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);

		// 2. 从 kafka 消息源获取数据, 直接将 value 当作行, 不管 key
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = -1087472118217411101L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		// 3. 分割出单词
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 3453365265664126459L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(SPACE.split(x)).iterator();
			}
		});

		// 4. 批内单词计数
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = -4945220237830302291L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 2586449647536031970L;

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
