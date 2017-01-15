package ljg;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class SparkStreaming {
	public static void main(String[] args) {
		 if (args.length < 4) {
		      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
		      System.exit(1);
		    }
		 SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		 JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		 int numThreads = Integer.parseInt(args[3]);
		 String[] topics = args[2].split(",");
		 Map<String, Integer> topicMap = new HashMap<>();
		    for (String topic: topics) {
		      topicMap.put(topic, numThreads);
		    }
		JavaPairReceiverInputDStream<String, String> messages =
		            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
		
		messages.print();
		jssc.start();
	    try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
