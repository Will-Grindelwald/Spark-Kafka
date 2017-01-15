package ljc.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDAO implements Closeable {

	private static HBaseDAO dao = null;
	private static Configuration config = null;
	private static Connection connection = null;
	private static Admin admin = null;

	private static final Log LOG = LogFactory.getLog(HBaseDAO.class);

	// 单例模式, 创建 connection 是一个很 heavy 的操作, 使用单例, 保证只创建一个 dao
	private HBaseDAO() throws IOException {
		config = HBaseConfiguration.create();
		// Add necessary configuration files
		config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));
		connection = ConnectionFactory.createConnection(config);
		admin = connection.getAdmin();
	}

	public static HBaseDAO getDao() throws IOException {
		if (dao == null) {
			dao = new HBaseDAO();
		}
		return dao;
	}

	public void close() throws IOException {
		admin.close();
		connection.close();
	}

	public Connection getConnection() {
		return connection;
	}

	public Configuration getConfig() {
		return config;
	}

	/**
	 * 列出所有表
	 * 
	 * @throws IOException
	 */
	public TableName[] listTableNames() throws IOException {
		return admin.listTableNames();
	}

	/**
	 * 判断表是否存在
	 * 
	 * @param tableNameStr
	 * @throws IOException
	 */
	public boolean tableExists(String tableNameStr) throws IOException {
		return admin.tableExists(TableName.valueOf(tableNameStr));
	}

	/**
	 * 判断表是否存在
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public boolean tableExists(TableName tableName) throws IOException {
		return admin.tableExists(tableName);
	}

	/**
	 * create a table
	 * 
	 * @param tableNameStr
	 * @param columnFamilys
	 * @throws IOException
	 */
	public void createTable(String tableNameStr, String... columnFamilys) throws IOException {
		createTable(tableNameStr, 1, null, columnFamilys);
	}

	/**
	 * create a table
	 * 
	 * @param tableNameStr
	 * @param maxVersions
	 * @param columnFamilys
	 * @throws IOException
	 */
	public void createTable(String tableNameStr, int maxVersions, String... columnFamilys) throws IOException {
		createTable(tableNameStr, maxVersions, null, columnFamilys);
	}

	/**
	 * create a table
	 * 
	 * @param tableNameStr
	 * @param maxVersions
	 * @param splitKeys
	 * @param columnFamilys
	 * @throws IOException
	 */
	public void createTable(String tableNameStr, int maxVersions, byte[][] splitKeys, String... columnFamilys)
			throws IOException {
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
	 * @param tableNameStr
	 * @throws IOException
	 */
	public void disableTable(String tableNameStr) throws IOException {
		disableTable(TableName.valueOf(tableNameStr));
	}

	/**
	 * disable table
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void disableTable(TableName tableName) throws IOException {
		admin.disableTable(tableName);
		LOG.info("Disable table: " + tableName.toString());
	}

	/**
	 * delete table, caution!!
	 * 
	 * @param tableNameStr
	 * @throws IOException
	 */
	public void deleteTable(String tableNameStr) throws IOException {
		TableName tableName = TableName.valueOf(tableNameStr);
		if (!admin.tableExists(tableName)) {
			LOG.error("Table: " + tableName + " does not exist.");
		} else {
			if (admin.isTableEnabled(tableName)) {
				disableTable(tableName);
			}
			admin.deleteTable(tableName);
			LOG.info("Delete table: " + tableName.toString());
		}
	}

	/**
	 * put a row
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @param value
	 * @throws IOException
	 */
	public void put(String tableNameStr, String rowKey, String columnFamily, String columnKey, String value)
			throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey), Bytes.toBytes(value));
			table.put(put);
		}
	}

	/**
	 * put a row with multi-column
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKeys
	 * @param values
	 * @throws IOException
	 */
	public void put(String tableNameStr, String rowKey, String columnFamily, String[] columnKeys, String[] values)
			throws IOException {
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
	 * @param tableNameStr
	 * @param rowKeys
	 * @param columnFamily
	 * @param columnKey
	 * @param values
	 * @throws IOException
	 */
	public void put(String tableNameStr, String[] rowKeys, String columnFamily, String columnKey, String[] values)
			throws IOException {
		if (rowKeys.length == values.length) {
			try (BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(tableNameStr));) {
				for (int i = 0; i < rowKeys.length; i++) {
					Put p = new Put(Bytes.toBytes(rowKeys[i]));
					p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey), Bytes.toBytes(values[i]));
					mutator.mutate(p);
					// System.out.println(mutator.getWriteBufferSize()); // 缓冲区大小, 由 hbase.client.write.buffer 决定
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
	 * @param tableNameStr
	 * @param put
	 * @throws IOException
	 */
	public void put(String tableNameStr, Put put) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.put(put);
		}
	}

	/**
	 * put a bench of rows
	 * 
	 * @param puts
	 * @throws IOException
	 */
	public void puts(String tableNameStr, List<Put> puts) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.put(puts);
		}
	}

	/**
	 * 判断 get 指定的列是否存在
	 * 
	 * @param tableNameStr
	 * @param get
	 * @throws IOException
	 */
	public void exists(String tableNameStr, Get get) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.exists(get);
		}
	}

	/**
	 * 判断 gets 指定的(多)列是否存在
	 * 
	 * @param tableNameStr
	 * @param gets
	 * @throws IOException
	 */
	public void existsALL(String tableNameStr, List<Get> gets) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			table.existsAll(gets);
		}
	}

	/**
	 * get a row identified by rowkey
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public Result get(String tableNameStr, String rowKey) throws IOException {
		return get(tableNameStr, rowKey, null, null);
	}

	/**
	 * get a row identified by rowkey
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public Result get(String tableNameStr, String rowKey, String columnFamily) throws IOException {
		return get(tableNameStr, rowKey, columnFamily, null);
	}

	/**
	 * get a row identified by rowkey
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public Result get(String tableNameStr, String rowKey, String columnFamily, String columnKey) throws IOException {
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
	 * @param tableNameStr
	 * @param get
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public Result get(String tableNameStr, Get get) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			return table.get(get);
		}
	}

	/**
	 * get rows
	 * 
	 * @param tableNameStr
	 * @param gets
	 * @return 若列不存在, 返回的 Result 不包含任何KeyValue, 用 Result.isEmpty() 鉴定.
	 * @throws IOException
	 */
	public Result[] gets(String tableNameStr, List<Get> gets) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			return table.get(gets);
		}
	}

	/**
	 * scan Table
	 * 
	 * @param tableNameStr
	 * @throws IOException
	 */
	public List<Result> scanTable(String tableNameStr) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr));
				ResultScanner resultScanner = table.getScanner(new Scan())) {
			List<Result> results = new ArrayList<Result>();
			for (Result result : resultScanner) {
				results.add(result);
			}
			return results;
		}
	}

	/**
	 * scan Table by start and stop row
	 * 
	 * @param tableNameStr
	 * @param startRow
	 * @param stopRow
	 * @throws IOException
	 */
	public List<Result> scanRange(String tableNameStr, String startRow, String stopRow) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr));
				ResultScanner resultScanner = table
						.getScanner(new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow)))) {
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
	public void printRecoder(Result result) {
		for (Cell cell : result.rawCells()) {
			System.out.println("Cell: " + cell + ", Value: "
					+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
		}
	}

	/**
	 * 删除指定行(所有列族的 所有列的 所有版本)
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @throws IOException
	 */
	public void deleteRow(String tableNameStr, String rowKey) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			table.delete(delete);
		}
	}

	/**
	 * 删除指定列族的所有列的所有版本
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @throws IOException
	 */
	public void deleteColumnFamily(String tableNameStr, String rowKey, String columnFamily) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addFamily(Bytes.toBytes(columnFamily));
			table.delete(delete);
		}
	}

	/**
	 * 删除指定列的所有版本
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @throws IOException
	 */
	public void deleteColumn(String tableNameStr, String rowKey, String columnFamily, String columnKey)
			throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableNameStr))) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey));
			table.delete(delete);
		}
	}

	/**
	 * 删除指定列的最后一个版本
	 * 
	 * @param tableNameStr
	 * @param rowKey
	 * @param columnFamily
	 * @param columnKey
	 * @throws IOException
	 */
	public void deleteColumnLast(String tableNameStr, String rowKey, String columnFamily, String columnKey)
			throws IOException {
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
	private static void modifySchema(Connection connection) throws IOException {
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
