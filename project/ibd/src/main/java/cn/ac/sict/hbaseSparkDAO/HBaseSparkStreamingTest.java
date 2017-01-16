package cn.ac.sict.hbaseSparkDAO;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 仅测试 streamBulkPut
 *
 */
public class HBaseSparkStreamingTest {

	public static final String zkQuorum = "192.168.125.171:2181,192.168.125.172:2181,192.168.125.173:2181";
	public static final String consumeGroup = "ljc_group";

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Usage: Main <topics> <numThreads> <tableName>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseStreamingBulkPutExample").set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		HBaseSparkDAO hDAO = HBaseSparkDAO.getDao(jsc);

		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(1000));
		JavaPairReceiverInputDStream<String, String> source = createStringSource(jssc, zkQuorum, consumeGroup, args[0],
				Integer.valueOf(args[1]));

		JavaDStream<String> massages = source.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 4669572391207668762L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		massages.print();
		HBaseSparkDAOUtil.streamBulkPutSimple(hDAO.getHbaseContext(), args[2], massages);

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static JavaPairReceiverInputDStream<String, String> createStringSource(JavaStreamingContext jssc,
			String zkQuorum, String consumeGroup, String topics, int numThreads) {
		Map<String, Integer> topicMap = new HashMap<>();
		for (String topic : topics.split(",")) {
			topicMap.put(topic, numThreads);
		}
		return KafkaUtils.createStream(jssc, zkQuorum, consumeGroup, topicMap);
	}
}
