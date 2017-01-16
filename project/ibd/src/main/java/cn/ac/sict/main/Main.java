package cn.ac.sict.main;

import java.io.IOException;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import cn.ac.sict.example.WordCount;
import cn.ac.sict.streamSource.KafkaStreamSource;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: Main <topics> <numThreads>");
			System.exit(1);
		}

		Properties configProps = new Properties();
		try {
			configProps.load(Main.class.getClassLoader().getResourceAsStream("sysConfig.properties"));
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("ERROR 1: no Config file.");
			return;
		}

		// 构建 Streaming Context 对象, 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("Spark").set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(1000));

		// 从 kafka 消息源获取数据
		JavaPairReceiverInputDStream<String, String> source = KafkaStreamSource.createStringSource(jssc,
				configProps.getProperty("zkQuorum"), configProps.getProperty("consumeGroup"), args[0],
				Integer.valueOf(args[1]));

		// 运行 wordCount
		WordCount.wordCount(source);

		// 启动 Spark Streaming
		jssc.start();
		jssc.awaitTermination();
	}

}
