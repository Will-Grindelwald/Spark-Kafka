package com.ljc.hbase.hbaseForSparkExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.ljc.hbase.HBaseDAOUtil;

/**
 * This is a simple example of getting records in HBase with the bulkGet
 * function.
 */
public class JavaHBaseBulkGetExample {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("JavaHBaseBulkGetExample  {tableName}");
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
			JavaRDD<Result> resultRDD = bulkGet(jsc, tableName, rdd);
			for (Result result : resultRDD.collect()) {
				System.out.println(HBaseDAOUtil.recoderToString(result));
			}
		} finally {
			jsc.stop();
		}
	}

	/**
	 * 批量 Get
	 */
	public static JavaRDD<Result> bulkGet(JavaSparkContext jsc, String tableName, JavaRDD<byte[]> getRowKeyRDD) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		int batchSize = getRowKeyRDD.count() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) getRowKeyRDD.count();
		return hbaseContext.bulkGet(TableName.valueOf(tableName), batchSize, getRowKeyRDD, new GetFunction(),
				new ResultFunction());
	}

	public static class GetFunction implements Function<byte[], Get> {
		private static final long serialVersionUID = -7417213135217098533L;

		public Get call(byte[] get) throws Exception {
			return new Get(get);
		}
	}

	public static class ResultFunction implements Function<Result, Result> {
		private static final long serialVersionUID = 1068702008076595392L;

		public Result call(Result result) throws Exception {
			return result;
		}
	}
}
