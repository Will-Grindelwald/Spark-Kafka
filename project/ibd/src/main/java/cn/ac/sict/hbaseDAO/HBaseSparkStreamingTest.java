package cn.ac.sict.hbaseDAO;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.ljc.sparkStreaming.template.KafkaStreamSource;

import scala.Tuple2;

/**
 * 目前只有 streamBulkPut
 *
 */
public class HBaseSparkStreamingTest {

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
		JavaPairReceiverInputDStream<String, String> source = KafkaStreamSource.createStringSource(jssc, args[0],
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
}
