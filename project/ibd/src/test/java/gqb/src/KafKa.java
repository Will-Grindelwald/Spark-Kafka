package gqb.src;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class KafKa {

	private static final String ZKQUORUM = "192.168.125.171:2181,192.168.125.172:2181,192.168.125.173:2181";

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {

		if (args.length < 3) {
			System.err.println("Usage: Kafka <consumer group id>, <per-topic number of Kafka partitions to consume> <numThreads>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStreaming");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		int numThreads = Integer.parseInt(args[2]);
		Map<String, Integer> topic = new HashMap<>();
		String[] topics = args[1].split(",");
		for (String t : topics) {
			topic.put(t, numThreads);
		}
		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jsc, ZKQUORUM, args[0],
				topic);

		JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
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
		jsc.start();
		jsc.awaitTermination();

	}
}
