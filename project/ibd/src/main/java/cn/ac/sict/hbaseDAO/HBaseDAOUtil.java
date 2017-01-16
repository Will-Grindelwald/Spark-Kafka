package cn.ac.sict.hbaseDAO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDAOUtil {

	private static final Log LOG = LogFactory.getLog(HBaseDAOUtil.class);

	/**
	 * 列出所有表
	 * 
	 * @param admin
	 * @throws IOException
	 */
	public static TableName[] listTableNames(Admin admin) throws IOException {
		return admin.listTableNames();
	}

	/**
	 * 判断表是否存在
	 * 
	 * @param admin
	 * @param tableNameStr
	 * @throws IOException
	 */
	public static boolean tableExists(Admin admin, String tableNameStr) throws IOException {
		return admin.tableExists(TableName.valueOf(tableNameStr));
	}

	/**
	 * 判断表是否存在
	 * 
	 * @param admin
	 * @param tableName
	 * @throws IOException
	 */
	public static boolean tableExists(Admin admin, TableName tableName) throws IOException {
		return admin.tableExists(tableName);
	}

	/**
	 * create a table
	 * 
	 * @param admin
	 * @param tableNameStr
	 * @param columnFamilys
	 * @throws IOException
	 */
	public static void createTable(Admin admin, String tableNameStr, String... columnFamilys) throws IOException {
		createTable(admin, tableNameStr, 1, null, columnFamilys);
	}

	/**
	 * create a table
	 * 
	 * @param admin
	 * @param tableNameStr
	 * @param maxVersions
	 * @param columnFamilys
	 * @throws IOException
	 */
	public static void createTable(Admin admin, String tableNameStr, int maxVersions, String... columnFamilys)
			throws IOException {
		createTable(admin, tableNameStr, maxVersions, null, columnFamilys);
	}

	/**
	 * create a table
	 * 
	 * @param admin
	 * @param tableNameStr
	 * @param maxVersions
	 * @param splitKeys
	 * @param columnFamilys
	 * @throws IOException
	 */
	public static void createTable(Admin admin, String tableNameStr, int maxVersions, byte[][] splitKeys,
			String... columnFamilys) throws IOException {
		TableName tableName = TableName.valueOf(tableNameStr);
		HTableDescriptor table = new HTableDescriptor(tableName);
		for (String columnFamily : columnFamilys) {
			table.addFamily(
					new HColumnDescriptor(columnFamily).setCompressionType(Algorithm.NONE).setMaxVersions(maxVersions));
		}
		if (splitKeys != null) {
			admin.createTable(table, splitKeys);
		} else {
			admin.createTable(table);
		}
		LOG.info("create table: " + tableNameStr);
	}

	/**
	 * disable table
	 * 
	 * @param admin
	 * @param tableNameStr
	 * @throws IOException
	 */
	public static void disableTable(Admin admin, String tableNameStr) throws IOException {
		disableTable(admin, TableName.valueOf(tableNameStr));
	}

	/**
	 * disable table
	 * 
	 * @param admin
	 * @param tableName
	 * @throws IOException
	 */
	public static void disableTable(Admin admin, TableName tableName) throws IOException {
		admin.disableTable(tableName);
		LOG.info("Disable table: " + tableName.toString());
	}

	/**
	 * delete table, caution!!
	 * 
	 * @param admin
	 * @param tableNameStr
	 * @throws IOException
	 */
	public static void deleteTable(Admin admin, String tableNameStr) throws IOException {
		TableName tableName = TableName.valueOf(tableNameStr);
		if (!admin.tableExists(tableName)) {
			LOG.error("Table: " + tableName + " does not exist.");
		} else {
			if (admin.isTableEnabled(tableName)) {
				disableTable(admin, tableName);
			}
			admin.deleteTable(tableName);
			LOG.info("Delete table: " + tableName.toString());
		}
	}

	/**
	 * put a row
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @param value
	 * @throws IOException
	 */
	public static void put(Connection connection, String tableNameStr, String rowKey, String columnFamily,
			String columnKey, String value) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey), Bytes.toBytes(value));
			table.put(put);
		}
	}

	/**
	 * put a row with multi-column
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKeys
	 * @param values
	 * @throws IOException
	 */
	public static void put(Connection connection, String tableNameStr, String rowKey, String columnFamily,
			String[] columnKeys, String[] values) throws IOException {
		if (columnKeys.length == values.length) {
			try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
				Put put = new Put(Bytes.toBytes(rowKey));
				for (int i = 0; i < columnKeys.length; i++) {
					put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKeys[i]), Bytes.toBytes(values[i]));
				}
				table.put(put);
			}
		} else {
			LOG.error("columnKeys.length: " + columnKeys.length + " != values.length" + values.length);
		}
	}

	/**
	 * put a bench of rows with same column, efficient
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKeys
	 * @param columnFamily
	 * @param columnKey
	 * @param values
	 * @throws IOException
	 */
	public static void put(Connection connection, String tableNameStr, String[] rowKeys, String columnFamily,
			String columnKey, String[] values) throws IOException {
		if (rowKeys.length == values.length) {
			try (BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(tableNameStr));) {
				for (int i = 0; i < rowKeys.length; i++) {
					Put p = new Put(Bytes.toBytes(rowKeys[i]));
					p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey), Bytes.toBytes(values[i]));
					mutator.mutate(p);
					// System.out.println(mutator.getWriteBufferSize()); //
					// 缓冲区大小, 由 hbase.client.write.buffer 决定
				}
				mutator.flush();
			}
		} else {
			LOG.error("rowKeys.lenght: " + rowKeys.length + " != values.length: " + values.length);
		}
	}

	/**
	 * put a row
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param put
	 * @throws IOException
	 */
	public static void put(Connection connection, String tableNameStr, Put put) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.put(put);
		}
	}

	/**
	 * put a bench of rows
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param puts
	 * @throws IOException
	 */
	public static void puts(Connection connection, String tableNameStr, List<Put> puts) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.put(puts);
		}
	}

	/**
	 * 判断 get 指定的列是否存在
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param get
	 * @throws IOException
	 */
	public static void exists(Connection connection, String tableNameStr, Get get) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.exists(get);
		}
	}

	/**
	 * 判断 gets 指定的(多)列是否存在
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param gets
	 * @throws IOException
	 */
	public static void existsALL(Connection connection, String tableNameStr, List<Get> gets) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.existsAll(gets);
		}
	}

	/**
	 * get a row identified by rowkey
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public static Result get(Connection connection, String tableNameStr, String rowKey) throws IOException {
		return get(connection, tableNameStr, rowKey, null, null);
	}

	/**
	 * get a row identified by rowkey
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public static Result get(Connection connection, String tableNameStr, String rowKey, String columnFamily)
			throws IOException {
		return get(connection, tableNameStr, rowKey, columnFamily, null);
	}

	/**
	 * get a row identified by rowkey
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public static Result get(Connection connection, String tableNameStr, String rowKey, String columnFamily,
			String columnKey) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Get get = new Get(Bytes.toBytes(rowKey));
			if (columnKey != null && columnFamily != null) {
				get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey));
			} else if (columnFamily != null) {
				get.addFamily(Bytes.toBytes(columnFamily));
			}
			return table.get(get);
		}
	}

	/**
	 * get a row
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param get
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public static Result get(Connection connection, String tableNameStr, Get get) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			return table.get(get);
		}
	}

	/**
	 * get rows
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param gets
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public static Result[] gets(Connection connection, String tableNameStr, List<Get> gets) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			return table.get(gets);
		}
	}

	/**
	 * scan Table
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @throws IOException
	 */
	public static List<Result> scanTable(Connection connection, String tableNameStr) throws IOException {
		return scanTable(connection, tableNameStr, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
	}

	/**
	 * scan Table by start and stop row
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param startRow
	 * @param stopRow
	 * @throws IOException
	 */
	public static List<Result> scanTable(Connection connection, String tableNameStr, String startRow, String stopRow)
			throws IOException {
		return scanTable(connection, tableNameStr, Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
	}

	/**
	 * scan Table by start and stop row
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param startRow
	 * @param stopRow
	 * @throws IOException
	 */
	public static List<Result> scanTable(Connection connection, String tableNameStr, byte[] startRow, byte[] stopRow)
			throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr));
				ResultScanner resultScanner = table.getScanner(new Scan(startRow, stopRow))) {
			List<Result> results = new ArrayList<Result>();
			for (Result result : resultScanner) {
				results.add(result);
			}
			return results;
		}
	}

	/**
	 * 
	 * @param result
	 */
	public static String recoderToString(Result result) {
		StringBuilder resultString = new StringBuilder();
		resultString.append("RowKey: " + Bytes.toString(result.getRow()));
		for (Cell cell : result.rawCells()) {
			resultString.append("\n\tColumnFamily: "
					+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
					+ ", Column: "
					+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
					+ ", Value: " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
					+ ", Timestamp:" + cell.getTimestamp());
		}
		return resultString.toString();
	}

	/**
	 * 删除指定行(所有列族的 所有列的 所有版本)
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @throws IOException
	 */
	public static void deleteRow(Connection connection, String tableNameStr, String rowKey) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			table.delete(delete);
		}
	}

	/**
	 * 删除指定列族的所有列的所有版本
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @throws IOException
	 */
	public static void deleteColumnFamily(Connection connection, String tableNameStr, String rowKey,
			String columnFamily) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addFamily(Bytes.toBytes(columnFamily));
			table.delete(delete);
		}
	}

	/**
	 * 删除指定列的所有版本
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @throws IOException
	 */
	public static void deleteColumn(Connection connection, String tableNameStr, String rowKey, String columnFamily,
			String columnKey) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey));
			table.delete(delete);
		}
	}

	/**
	 * 删除指定列的最后一个版本
	 * 
	 * @param connection
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @throws IOException
	 */
	public static void deleteColumnLast(Connection connection, String tableNameStr, String rowKey, String columnFamily,
			String columnKey) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey));
			table.delete(delete);
		}
	}

	/**
	 * 此为示例代码, 一般不建议在代码中更改表结构
	 * 
	 * @param connection
	 * @throws IOException
	 */
	public static void modifySchema(Connection connection) throws IOException {
		try (Admin admin = connection.getAdmin()) {

			TableName tableName = TableName.valueOf("test");
			if (!admin.tableExists(tableName)) {
				System.out.println("Table does not exist.");
				System.exit(-1);
			}

			HTableDescriptor table = new HTableDescriptor(tableName);

			// Update existing table
			HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
			newColumn.setCompactionCompressionType(Algorithm.GZ);
			newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
			admin.addColumn(tableName, newColumn);

			// Update existing column family
			HColumnDescriptor existingColumn = new HColumnDescriptor("testcf");
			existingColumn.setCompactionCompressionType(Algorithm.GZ);
			existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
			table.modifyFamily(existingColumn);
			admin.modifyTable(tableName, table);

			// Disable an existing table
			admin.disableTable(tableName);

			// Delete an existing column family
			admin.deleteColumn(tableName, "testcf".getBytes("UTF-8"));

			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableName);
		}
	}

}
