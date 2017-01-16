package cn.ac.sict.Vis;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Site {

	private static final SparkSession session = SparkSession.builder().getOrCreate();;
	private Dataset<Row> jdbcDF;
	private static List<String> views;

	public Site() {
		views = new ArrayList<>();
		jdbcDF = session.read().format("jdbc").option("url", "jdbc:mysql://192.168.125.171:3306/SITE")
				.option("dbtable", "SITE").option("driver", "com.mysql.jdbc.Driver").option("user", "root")
				.option("password", "sict").load();
	}

	public Dataset<Row> getData() {
		return jdbcDF;
	}

	public SparkSession getSparkSession() {
		return session;
	}

	public void createOrReplaceTempView(String name) {
		jdbcDF.createOrReplaceTempView(name);
		views.add(name);
	}

	public Dataset<Row> sqlData(String sql) {
		if (views.isEmpty()) {
			System.err.println("Need Register the DataFrame as a SQL temporary view");
			return null;
		}
		Dataset<Row> sqldata = session.sql(sql);
		return sqldata;
	}

	public List<Integer> getList() {
		createOrReplaceTempView("sql_tmp");
		Dataset<Row> sql = session.sql("select sensorid from sql_tmp");
		List<Row> list = sql.collectAsList();
		List<Integer> list2 = new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			list2.add(list.get(i).getInt(0));
		}
		return list2;
	}

}
