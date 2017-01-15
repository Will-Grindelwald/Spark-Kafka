package com.ljc.hbase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import scala.Tuple2;

public class HBaseSparkDAOUtil {

	/**
	 * 简易批量 Put for JavaRDD<String> putRdd, String 的格式为
	 * "rowKey,columnFamily,columnKey,value"
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param putRDD
	 */
	public static void bulkPutSimple(JavaHBaseContext hbaseContext, String tableName, JavaRDD<String> putRDD) {
		bulkPut(hbaseContext, tableName, putRDD, new PutFunction1());
	}

	/**
	 * 批量 Put for JavaRDD<T> putRdd
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param putRDD
	 * @param PutFunction
	 *            Function to convert <T> in the JavaRDD to a HBase Put
	 */
	public static <T> void bulkPut(JavaHBaseContext hbaseContext, String tableName, JavaRDD<T> putRDD,
			Function<T, Put> PutFunction) {
		hbaseContext.bulkPut(putRDD, TableName.valueOf(tableName), PutFunction);
	}

	/**
	 * 简易批量 Put for JavaDStream<String> putDStream, String 的格式为
	 * "rowKey,columnFamily,columnKey,value"
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param putDStream
	 */
	public static void streamBulkPutSimple(JavaHBaseContext hbaseContext, String tableName,
			JavaDStream<String> putDStream) {
		streamBulkPut(hbaseContext, tableName, putDStream, new PutFunction1());
	}

	/**
	 * 批量 Put for JavaDStream<T> putDStream
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param putDStream
	 * @param PutFunction
	 *            Function to convert <T> in the JavaDStream to a HBase Puts
	 */
	public static <T> void streamBulkPut(JavaHBaseContext hbaseContext, String tableName, JavaDStream<T> putDStream,
			Function<T, Put> PutFunction) {
		hbaseContext.streamBulkPut(putDStream, TableName.valueOf(tableName), PutFunction);
	}

	public static class PutFunction1 implements Function<String, Put> {
		private static final long serialVersionUID = 8038080193924048202L;

		public Put call(String put) throws Exception {
			String[] args = put.split(",");
			return new Put(Bytes.toBytes(args[0])).addColumn(Bytes.toBytes(args[1]), Bytes.toBytes(args[2]),
					Bytes.toBytes(args[3]));
		}
	}

	/**
	 * 简易批量 Delete for JavaRDD<String>, String 为 RowKey
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param deleteRowKeyRDD
	 */
	public static void bulkDeleteSimple(JavaHBaseContext hbaseContext, String tableName,
			JavaRDD<String> deleteRowKeyRDD) {
		bulkDelete(hbaseContext, tableName, deleteRowKeyRDD, new DeleteFunction1());
	}

	/**
	 * 批量 Delete for JavaRDD<T>, String 为 RowKey
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param deleteRowKeyRDD
	 * @param DeleteFunction
	 *            Function to convert <T> in the JavaRDD to a HBase Deletes
	 */
	public static <T> void bulkDelete(JavaHBaseContext hbaseContext, String tableName, JavaRDD<T> deleteRowKeyRDD,
			Function<T, Delete> DeleteFunction) {
		int batchSize = deleteRowKeyRDD.count() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) deleteRowKeyRDD.count();
		hbaseContext.bulkDelete(deleteRowKeyRDD, TableName.valueOf(tableName), DeleteFunction, batchSize);
	}

	/**
	 * 简易批量 Delete for JavaDStream<String>, String 为 RowKey
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param deleteRowKeyDStream
	 */
	public static void streamBulkDeleteSimple(JavaHBaseContext hbaseContext, String tableName,
			JavaDStream<String> deleteRowKeyDStream) {
		streamBulkDelete(hbaseContext, tableName, deleteRowKeyDStream, new DeleteFunction1());
	}

	/**
	 * 批量 Delete for JavaDStream<T>, String 为 RowKey
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param deleteRowKeyDStream
	 * @param DeleteFunction
	 *            DeleteFunction Function to convert <T> in the JavaDStream to a
	 *            HBase Deletes
	 */
	public static <T> void streamBulkDelete(JavaHBaseContext hbaseContext, String tableName,
			JavaDStream<T> deleteRowKeyDStream, Function<T, Delete> DeleteFunction) {
		hbaseContext.streamBulkDelete(deleteRowKeyDStream, TableName.valueOf(tableName), DeleteFunction,
				Integer.MAX_VALUE);
	}

	public static class DeleteFunction1 implements Function<String, Delete> {
		private static final long serialVersionUID = -6770814881834435096L;

		public Delete call(String deleteRowKey) throws Exception {
			return new Delete(Bytes.toBytes(deleteRowKey));
		}
	}

	/**
	 * 简易批量 Get for JavaRDD<String>, String 为 RowKey
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param getRowKeyRDD
	 * @return JavaRDD<Result>
	 */
	public static JavaRDD<Result> bulkGetSimple(JavaHBaseContext hbaseContext, String tableName,
			JavaRDD<String> getRowKeyRDD) {
		return bulkGet(hbaseContext, tableName, getRowKeyRDD, new GetFunction1(), new ResultFunction1());
	}

	/**
	 * 批量 Get for JavaRDD<T>
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param getRowKeyRDD
	 * @param GetFunction
	 *            Function to convert <T> in the JavaRDD to a HBase Get
	 * @param ResultFunction
	 *            This will convert the HBase Result object to what ever the
	 *            user wants to put in the resulting JavaRDD
	 * @return JavaRDD<U>
	 */
	public static <T, U> JavaRDD<U> bulkGet(JavaHBaseContext hbaseContext, String tableName, JavaRDD<T> getRowKeyRDD,
			Function<T, Get> GetFunction, Function<Result, U> ResultFunction) {
		int batchSize = getRowKeyRDD.count() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) getRowKeyRDD.count();
		return hbaseContext.bulkGet(TableName.valueOf(tableName), batchSize, getRowKeyRDD, GetFunction, ResultFunction);
	}

	/**
	 * 简易批量 Get for JavaDStream<String>, String 为 RowKey 目前有问题
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param getRowKeyDStream
	 * @return JavaDStream<Result>
	 */
	/*public static JavaDStream<Result> streamBulkGetSimple(JavaHBaseContext hbaseContext, String tableName,
			JavaDStream<String> getRowKeyDStream) {
		return streamBulkGet(hbaseContext, tableName, getRowKeyDStream, new GetFunction1(), new ResultFunction1());
	}*/

	/**
	 * 批量 Get for JavaDStream<T> 目前有问题
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param getRowKeyDStream
	 * @param GetFunction
	 *            Function to convert <T> in the JavaDStream to a HBase Get
	 * @param ResultFunction
	 *            This will convert the HBase Result object to what ever the
	 *            user wants to put in the resulting JavaDStream
	 * @return JavaDStream<U>
	 */
	/*public static <T, U> JavaDStream<U> streamBulkGet(JavaHBaseContext hbaseContext, String tableName,
			JavaDStream<T> getRowKeyDStream, Function<T, Get> GetFunction, Function<Result, U> ResultFunction) {
		return hbaseContext.streamBulkGet(TableName.valueOf(tableName), Integer.MAX_VALUE, getRowKeyDStream,
				GetFunction, ResultFunction);
	}*/

	public static class GetFunction1 implements Function<String, Get> {
		private static final long serialVersionUID = -7417213135217098533L;

		public Get call(String get) throws Exception {
			return new Get(Bytes.toBytes(get));
		}
	}

	public static class ResultFunction1 implements Function<Result, Result> {
		private static final long serialVersionUID = 1068702008076595392L;

		public Result call(Result result) throws Exception {
			return result;
		}
	}

	/**
	 * 简易 Scan
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @return JavaRDD<Result>
	 */
	public static JavaRDD<Result> DistributedScanSimple(JavaHBaseContext hbaseContext, String tableName) {
		return DistributedScan(hbaseContext, tableName, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
				new ScanConvertFunction1());
	}

	/**
	 * 简易 Scan by start and stop row
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param startRow
	 * @param stopRow
	 * @return JavaRDD<Result>
	 */
	public static JavaRDD<Result> DistributedScanSimple(JavaHBaseContext hbaseContext, String tableName,
			String startRow, String stopRow) {
		return DistributedScan(hbaseContext, tableName, Bytes.toBytes(startRow), Bytes.toBytes(stopRow),
				new ScanConvertFunction1());
	}

	/**
	 * Scan by start and stop row
	 * 
	 * @param hbaseContext
	 * @param tableName
	 * @param startRow
	 * @param stopRow
	 * @param ScanConvertFunction
	 *            Function to convert a Result object from HBase into What the
	 *            user wants in the final generated JavaRDD
	 * @return JavaRDD<Result>
	 */
	public static <T> JavaRDD<T> DistributedScan(JavaHBaseContext hbaseContext, String tableName, byte[] startRow,
			byte[] stopRow, Function<Tuple2<ImmutableBytesWritable, Result>, T> ScanConvertFunction) {
		Scan scan = new Scan(startRow, stopRow).setCaching(100);

		return hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan, ScanConvertFunction);
	}

	private static class ScanConvertFunction1 implements Function<Tuple2<ImmutableBytesWritable, Result>, Result> {
		private static final long serialVersionUID = -5548312950815838132L;

		@Override
		public Result call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
			return v1._2();
		}
	}
}
