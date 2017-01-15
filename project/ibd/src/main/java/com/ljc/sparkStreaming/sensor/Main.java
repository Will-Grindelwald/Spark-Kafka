package com.ljc.sparkStreaming.sensor;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: Main <topics> <numThreads>");
			System.exit(1);
		}

		// 构建 Streaming Context 对象, 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

		// 从 kafka 消息源获取数据
		JavaPairReceiverInputDStream<String, String> source = KafkaStreamSource.createStringSource(jssc, args[0],
				Integer.valueOf(args[1]));

		// 运行 wordCount
		// WordCount.wordCount(source);

		// 启动 Spark Streaming
		jssc.start();
		jssc.awaitTermination();
	}

}
