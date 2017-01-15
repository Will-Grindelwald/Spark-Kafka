package ljg;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.Iterator;
import java.util.Arrays;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
public class KafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) {
		 if (args.length < 4) {
		      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
		      System.exit(1);
		    }
		 SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		 JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		 int numThreads = Integer.parseInt(args[3]);
		 String[] topics = args[2].split(",");
		 Map<String, Integer> topicMap = new HashMap<>();
		    for (String topic: topics) {
		      topicMap.put(topic, numThreads);
		    }
		JavaPairReceiverInputDStream<String, String> messages =
		            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
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

		    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
		      new PairFunction<String, String, Integer>() {
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
		    jssc.start();
		    try {
				jssc.awaitTermination();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
