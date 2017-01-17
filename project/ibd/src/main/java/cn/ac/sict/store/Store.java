package cn.ac.sict.store;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;

import com.google.gson.Gson;

import cn.ac.sict.hbaseSparkDAO.HBaseSparkDAO;
import cn.ac.sict.hbaseSparkDAO.HBaseSparkDAOUtil;
import cn.ac.sict.signal.Signal;
import cn.ac.sict.signal.TemperSignal;
import scala.Tuple2;

public class Store {

	/**
	 * 
	 * @param hDAO
	 * @param tableNameStr
	 * @param columnFamily
	 * @param source
	 * @param classOfT
	 *            T extends Signal
	 */
	public static <T extends Signal> void toHBase(HBaseSparkDAO hDAO, String tableNameStr, String columnFamily,
			JavaPairReceiverInputDStream<String, String> source, Class<T> classOfT) {
		JavaDStream<Signal> signalDStream = getSignal(source, classOfT);
		//
		signalDStream.print();
		HBaseSparkDAOUtil.streamBulkPut(hDAO.getHbaseContext(), tableNameStr, signalDStream,
				new PutFunction(columnFamily));
	}

	/**
	 * 
	 * @param source
	 * @param classOfT
	 * @return
	 */
	public static <T extends Signal> JavaDStream<Signal> getSignal(JavaPairReceiverInputDStream<String, String> source,
			final Class<T> classOfT) {
		return getValue(source).map(new Function<String, Signal>() {
			private static final long serialVersionUID = -7859770045664622717L;

			@Override
			public Signal call(String json) throws Exception {
				return new Gson().fromJson(json, classOfT);
			}
		});
	}

	/**
	 * 
	 * @param source
	 * @return
	 */
	public static JavaDStream<String> getValue(JavaPairReceiverInputDStream<String, String> source) {
		return source.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = -5944036348426266550L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
	}

	/**
	 * 将 Signal 实例转换成 Put, 目前只有 TemperSignal, 未来添加新的 Signal, 必须修改 call 方法
	 *
	 */
	public static class PutFunction implements Function<Signal, Put> {
		private static final long serialVersionUID = 4445645466633206640L;

		private String columnFamily;

		public PutFunction(String columnFamily) {
			this.columnFamily = columnFamily;
		}

		public Put call(Signal signal) throws Exception {
			Put put = new Put(Bytes.toBytes(String.valueOf(signal.id)));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("time"),
					Bytes.toBytes(String.valueOf(signal.time)));
			if (signal instanceof TemperSignal) {
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("temper"),
						Bytes.toBytes(String.valueOf(((TemperSignal) signal).temper)));
			} else {
				System.err.println("ERROR 2: unknow signal");
			}
			return put;
		}
	}
}
