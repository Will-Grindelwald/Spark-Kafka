package com.ljc.sparkStreaming.sensor;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class KafkaStreamSource {

	private static final String ZKQUORUM = "192.168.125.171:2181,192.168.125.172:2181,192.168.125.173:2181";
	private static final String GROUP = "ljc_group";

	public static JavaPairReceiverInputDStream<String, String> createStringSource(JavaStreamingContext ssc,
			String topics, int numThreads) {
		Map<String, Integer> topicMap = new HashMap<>();
		for (String topic : topics.split(",")) {
			topicMap.put(topic, numThreads);
		}

		return KafkaUtils.createStream(ssc, ZKQUORUM, GROUP, topicMap);
	}

}
