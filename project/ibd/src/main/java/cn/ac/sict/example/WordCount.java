package cn.ac.sict.example;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;

import scala.Tuple2;

public class WordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void wordCount(JavaPairReceiverInputDStream<String, String> source) {
		// 直接将 value 当作行, 不管 key
		JavaDStream<String> lines = source.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = -1723119776885570246L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		// 分割出单词
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = -5367254022770599468L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(SPACE.split(x)).iterator();
			}
		});

		// 批内单词计数
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = -7191328106297846270L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 5821667678406494267L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
	}
}
