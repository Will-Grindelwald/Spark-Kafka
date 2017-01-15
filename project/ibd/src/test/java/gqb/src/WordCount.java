package gqb.src;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public final class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		// if (args.length < 2) {
		// System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
		// System.exit(1);
		// }

		// Create the context with a 1 second batch size
//		SparkConf sparkConf = new SparkConf().setMaster("mesos://192.168.125.171:8081")
//				.setAppName("JavaNetworkWordCount").set("spark.executor.uri", "/home/null/spark.tar.gz")
//				.set("spark.shuffle.blockTransferService", "nio");
		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaNetworkWordCount")
				.set("spark.shuffle.blockTransferService", "nio");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		// Create a JavaReceiverInputDStream on target ip:port and count the
		// words in input stream of \n delimited text (eg. generated by 'nc')
		// Note that no duplication in storage level only for running locally.
		// Replication necessary in distributed scenario for fault tolerance.
		// JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
		// args[0], Integer.parseInt(args[1]));
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream("192.168.125.171", 9988);
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(SPACE.split(x)).iterator();
			}
		});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		wordCounts.print();
		ssc.start();
		ssc.awaitTermination();
	}
}