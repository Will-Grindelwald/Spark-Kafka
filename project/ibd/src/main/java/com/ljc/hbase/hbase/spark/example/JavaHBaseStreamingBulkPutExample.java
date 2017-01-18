package com.ljc.hbase.hbase.spark.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.ljc.spark.streaming.template.KafkaStreamSource;

import scala.Tuple2;

/**
 * This is a simple example of BulkPut with Spark Streaming
 */
public class JavaHBaseStreamingBulkPutExample {

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 3) {
			System.err.println("Usage: Main <topics> <numThreads> <tableName>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseStreamingBulkPutExample").set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
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

		streamBulkPut(jsc, args[2], massages);

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 批量 Put JavaDStream<String> putDStream 中 String 的格式为
	 * "rowKey,columnFamily,columnKey,value"
	 */
	public static void streamBulkPut(JavaSparkContext jsc, String tableName, JavaDStream<String> putDStream) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		hbaseContext.streamBulkPut(putDStream, TableName.valueOf(tableName), new PutFunction());
	}

	public static class PutFunction implements Function<String, Put> {
		private static final long serialVersionUID = 8038080193924048202L;

		public Put call(String put) throws Exception {
			String[] args = put.split(",");
			return new Put(Bytes.toBytes(args[0])).addColumn(Bytes.toBytes(args[1]), Bytes.toBytes(args[2]),
					Bytes.toBytes(args[3]));
		}
	}
}
