package cn.ac.sict.store;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;

public class PutStrWrapper {
	private String rowKey;
	private String columnFamily;
	private String[] columnKeys;
	private String[] values;

	public PutStrWrapper(String json) {
	}

	public String getRowKey() {
		return rowKey;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public String[] getColumnKeys() {
		return columnKeys;
	}

	public String[] getValues() {
		return values;
	}

	public boolean setColumnKeysAndValues(String[] columnKeys, String[] values) {
		if (columnKeys.length != values.length) {
			return false;
		}
		this.columnKeys = columnKeys;
		this.values = values;
		return true;
	}

	public static class PutFunction implements Function<PutStrWrapper, Put> {
		private static final long serialVersionUID = 2869462271063520599L;

		public Put call(PutStrWrapper putStrWrapper) throws Exception {
			Put put = new Put(Bytes.toBytes(putStrWrapper.rowKey));
			for (int i = 0; i < putStrWrapper.columnKeys.length; i++) {
				put.addColumn(Bytes.toBytes(putStrWrapper.columnFamily), Bytes.toBytes(putStrWrapper.columnKeys[i]),
						Bytes.toBytes(putStrWrapper.values[i]));
			}
			return put;
		}
	}
}
