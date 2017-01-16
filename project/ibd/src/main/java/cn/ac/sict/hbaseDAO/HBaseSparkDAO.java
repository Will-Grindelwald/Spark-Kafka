package cn.ac.sict.hbaseDAO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.api.java.JavaSparkContext;

public class HBaseSparkDAO {

	private static HBaseSparkDAO dao = null;
	private static Configuration config = null;
	private static JavaHBaseContext hbaseContext;

	// 单例模式
	private HBaseSparkDAO(JavaSparkContext jsc) throws IOException {
		config = HBaseConfiguration.create();
		// Add necessary configuration files
		config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));
		hbaseContext = new JavaHBaseContext(jsc, config);
	}

	public static HBaseSparkDAO getDao(JavaSparkContext jsc) throws IOException {
		if (dao == null) {
			dao = new HBaseSparkDAO(jsc);
		}
		return dao;
	}

	public Configuration getConfig() {
		return config;
	}

	public JavaHBaseContext getHbaseContext() {
		return hbaseContext;
	}

}
