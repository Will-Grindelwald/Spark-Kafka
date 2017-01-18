package com.ljc.hbase.hbase.spark.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a simple example of deleting records in HBase with the bulkDelete
 * function.
 */
public class JavaHBaseBulkDeleteExample {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("JavaHBaseBulkDeleteExample  {tableName}");
			return;
		}

		String tableName = args[0];

		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaHBaseBulkGetExample " + tableName)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		List<byte[]> list = new ArrayList<>();
		list.add(Bytes.toBytes("1"));
		list.add(Bytes.toBytes("2"));
		list.add(Bytes.toBytes("3"));
		list.add(Bytes.toBytes("4"));
		list.add(Bytes.toBytes("5"));

		JavaRDD<byte[]> rdd = jsc.parallelize(list);

		try {
			bulkDelete(jsc, tableName, rdd);
		} finally {
			jsc.stop();
		}
	}

	/**
	 * 批量 Delete
	 */
	public static void bulkDelete(JavaSparkContext jsc, String tableName, JavaRDD<byte[]> deleteRowKeyRDD) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		int batchSize = deleteRowKeyRDD.count() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) deleteRowKeyRDD.count();
		hbaseContext.bulkDelete(deleteRowKeyRDD, TableName.valueOf(tableName), new DeleteFunction(), batchSize);
	}

	public static class DeleteFunction implements Function<byte[], Delete> {
		private static final long serialVersionUID = -6770814881834435096L;

		public Delete call(byte[] delete) throws Exception {
			return new Delete(delete);
		}
	}
}
