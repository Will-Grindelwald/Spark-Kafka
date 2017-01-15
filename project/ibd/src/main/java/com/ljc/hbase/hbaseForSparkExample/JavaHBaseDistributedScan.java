package com.ljc.hbase.hbaseForSparkExample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.ljc.hbase.HBaseDAOUtil;

import scala.Tuple2;

/**
 * This is a simple example of scanning records from HBase with the hbaseRDD
 * function.
 */
public class JavaHBaseDistributedScan {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("JavaHBaseDistributedScan {tableName}");
			return;
		}

		String tableName = args[0];

		SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseDistributedScan " + tableName)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		try {
			JavaRDD<Result> resultRDD = DistributedScan(jsc, tableName);
			for (Result result : resultRDD.collect()) {
				System.out.println(HBaseDAOUtil.recoderToString(result));
			}
		} finally {
			jsc.stop();
		}
	}

	public static JavaRDD<Result> DistributedScan(JavaSparkContext jsc, String tableName) {
		return DistributedScan(jsc, tableName, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
	}

	public static JavaRDD<Result> DistributedScan(JavaSparkContext jsc, String tableName, String startRow,
			String stopRow) {
		return DistributedScan(jsc, tableName, Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
	}

	public static JavaRDD<Result> DistributedScan(JavaSparkContext jsc, String tableName, byte[] startRow,
			byte[] stopRow) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		Scan scan = new Scan(startRow, stopRow).setCaching(100);

		return hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan, new ScanConvertFunction());
	}

	private static class ScanConvertFunction implements Function<Tuple2<ImmutableBytesWritable, Result>, Result> {
		private static final long serialVersionUID = -5548312950815838132L;

		@Override
		public Result call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
			return v1._2();
		}
	}
}
