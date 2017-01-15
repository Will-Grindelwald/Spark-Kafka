package com.ljc.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HBaseSparkTest {

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("JavaHBaseBulkPutExample <tableName> <columnFamily>");
			return;
		}

		String tableName = args[0];
		String columnFamily = args[1];

		SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkGetExample " + tableName).set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		HBaseSparkDAO hDAO = HBaseSparkDAO.getDao(jsc);

		// 1. Put
		List<String> listPut = new ArrayList<>();
		listPut.add("1," + columnFamily + ",a,1");
		listPut.add("2," + columnFamily + ",a,2");
		listPut.add("3," + columnFamily + ",a,3");
		listPut.add("4," + columnFamily + ",a,4");
		listPut.add("5," + columnFamily + ",a,5");

		JavaRDD<String> putRdd = jsc.parallelize(listPut);

		HBaseSparkDAOUtil.bulkPutSimple(hDAO.getHbaseContext(), tableName, putRdd);

		// 2. Get
		List<String> listGet = new ArrayList<>();
		listGet.add("1");
		listGet.add("2");
		listGet.add("3");
		listGet.add("4");
		listGet.add("5");

		JavaRDD<String> getRdd = jsc.parallelize(listGet);

		JavaRDD<Result> getResultRDD = HBaseSparkDAOUtil.bulkGetSimple(hDAO.getHbaseContext(), tableName, getRdd);
		for (Result result : getResultRDD.collect()) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		// 3. DistributedScan
		JavaRDD<Result> scanResultRDD = HBaseSparkDAOUtil.DistributedScanSimple(hDAO.getHbaseContext(), tableName);
		for (Result result : scanResultRDD.collect()) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		// 4. Delete
		List<String> listDelete = new ArrayList<>();
		listDelete.add("1");
		listDelete.add("2");
		listDelete.add("3");
		listDelete.add("4");
		listDelete.add("5");

		JavaRDD<String> deleteRdd = jsc.parallelize(listDelete);

		HBaseSparkDAOUtil.bulkDeleteSimple(hDAO.getHbaseContext(), tableName, deleteRdd);
	}
}
