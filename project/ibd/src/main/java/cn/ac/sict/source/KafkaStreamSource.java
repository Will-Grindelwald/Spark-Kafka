package cn.ac.sict.source;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class KafkaStreamSource {

	public static JavaPairReceiverInputDStream<String, String> createStringSource(JavaStreamingContext jssc,
			String zkQuorum, String consumeGroup, String topics, int numThreads) {
		Map<String, Integer> topicMap = new HashMap<>();
		for (String topic : topics.split(",")) {
			topicMap.put(topic, numThreads);
		}
		return KafkaUtils.createStream(jssc, zkQuorum, consumeGroup, topicMap);
	}
}
