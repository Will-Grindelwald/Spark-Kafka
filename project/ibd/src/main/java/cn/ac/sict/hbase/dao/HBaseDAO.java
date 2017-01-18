package cn.ac.sict.hbase.dao;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseDAO implements Closeable {

	private static HBaseDAO dao = null;
	private static Configuration config = null;
	private static Connection connection = null;
	private static Admin admin = null;

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

	public Configuration getConfig() {
		return config;
	}

	public Connection getConnection() {
		return connection;
	}

	public Admin getAdmin() {
		return admin;
	}

}
