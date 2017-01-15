package com.ljc.hbase.hbaseForSparkExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * This is a simple example of putting records in HBase with the bulkPut
 * function.
 */
public class JavaHBaseBulkPutExample {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("JavaHBaseBulkPutExample  " + "{tableName} {columnFamily}");
			return;
		}

		String tableName = args[0];
		String columnFamily = args[1];

		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaHBaseBulkGetExample " + tableName)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		List<String> list = new ArrayList<>();
		list.add("1," + columnFamily + ",a,1");
		list.add("2," + columnFamily + ",a,2");
		list.add("3," + columnFamily + ",a,3");
		list.add("4," + columnFamily + ",a,4");
		list.add("5," + columnFamily + ",a,5");

		JavaRDD<String> rdd = jsc.parallelize(list);

		try {
			bulkPut(jsc, tableName, rdd);
		} finally {
			jsc.stop();
		}
	}

	/**
	 * 批量 Put JavaRDD<String> putRdd 中 String 的格式为
	 * "rowKey,columnFamily,columnKey,value"
	 */
	public static void bulkPut(JavaSparkContext jsc, String tableName, JavaRDD<String> putRDD) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		hbaseContext.bulkPut(putRDD, TableName.valueOf(tableName), new PutFunction());
	}

	public static class PutFunction implements Function<String, Put> {
		private static final long serialVersionUID = 8038080193924048202L;

		public Put call(String put) throws Exception {
			String[] args = put.split(",");
			return new Put(Bytes.toBytes(args[0])).addColumn(Bytes.toBytes(args[1]), Bytes.toBytes(args[2]), Bytes.toBytes(args[3]));
		}
	}
}
