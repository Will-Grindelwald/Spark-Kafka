package com.ljc.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;

public class HBaseDAOTest {

	public static void main(String[] args) throws IOException {

		HBaseDAO hBaseDAO = HBaseDAO.getDao();
		// 列出所有的表
		System.out.println("1. listTableNames");
		TableName[] tableNames = HBaseDAOUtil.listTableNames(hBaseDAO.getAdmin());
		for (TableName tableName : tableNames) {
			System.out.println("Table Name is : " + tableName.getNameAsString());
		}

		// 判断表是否存在
		System.out.println("2. tableExists");
		boolean exists = HBaseDAOUtil.tableExists(hBaseDAO.getAdmin(), "test");

		// 存在就删除
		System.out.println("3. deleteTable");
		if (exists) {
			HBaseDAOUtil.disableTable(hBaseDAO.getAdmin(), "test");
			HBaseDAOUtil.deleteTable(hBaseDAO.getAdmin(), "test");
			System.out.println("delete 'test'");
		}

		// 创建表
		System.out.println("4. createTable");
		HBaseDAOUtil.createTable(hBaseDAO.getAdmin(), "test", "testcf");
		System.out.println("create 'test'");

		// 再次列出所有的表
		System.out.println("5. listTableNames");
		tableNames = HBaseDAOUtil.listTableNames(hBaseDAO.getAdmin());
		for (TableName tableName : tableNames) {
			System.out.println("Table Name is : " + tableName.getNameAsString());
		}

		// 添加数据
		System.out.println("6. put");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row1", "testcf", "a", "1");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row1", "testcf", "a", "2");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row1", "testcf", "a", "3");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row1", "testcf", "b", "1");

		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row2", "testcf", "a", "1");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row2", "testcf", "a", "2");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row2", "testcf", "a", "3");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row2", "testcf", "b", "1");
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row2", "testcf", "c", "1");

		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row3", "testcf", new String[] { "a", "b", "c" },
				new String[] { "1", "1", "1" });
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", "row3", "testcf", new String[] { "a", "b", "c" },
				new String[] { "2", "2", "2" });
		HBaseDAOUtil.put(hBaseDAO.getConnection(), "test", new String[] { "row1", "row2", "row3" }, "testcf", "z",
				new String[] { "z1", "z2", "z3" });

		// 检索数据-表扫描
		System.out.println("7. scanTable");
		List<Result> results = HBaseDAOUtil.scanTable(hBaseDAO.getConnection(), "test");
		for (Result result : results) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		System.out.println("8. scanRange");
		results = HBaseDAOUtil.scanTable(hBaseDAO.getConnection(), "test", "row1", "row2");
		for (Result result : results) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		// 检索数据-获取单行
		System.out.println("9. get");
		Result getResult = HBaseDAOUtil.get(hBaseDAO.getConnection(), "test", "row1");
		System.out.println(HBaseDAOUtil.recoderToString(getResult));

		// 删除数据
		System.out.println("10. deleteRow");
		HBaseDAOUtil.deleteRow(hBaseDAO.getConnection(), "test", "row1"); // 删除指定行
		results = HBaseDAOUtil.scanTable(hBaseDAO.getConnection(), "test");
		for (Result result : results) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		System.out.println("11. deleteColumnFamily");
		HBaseDAOUtil.deleteColumnFamily(hBaseDAO.getConnection(), "test", "row2", "testcf"); // 删除指定列族的所有列的所有版本
		results = HBaseDAOUtil.scanTable(hBaseDAO.getConnection(), "test");
		for (Result result : results) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		System.out.println("12. deleteColumn");
		HBaseDAOUtil.deleteColumn(hBaseDAO.getConnection(), "test", "row3", "testcf", "b"); // 删除指定列的所有版本
		results = HBaseDAOUtil.scanTable(hBaseDAO.getConnection(), "test");
		for (Result result : results) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		System.out.println("13. deleteColumnLast");
		HBaseDAOUtil.deleteColumnLast(hBaseDAO.getConnection(), "test", "row3", "testcf", "a"); // 删除指定列的最后一个版本
		results = HBaseDAOUtil.scanTable(hBaseDAO.getConnection(), "test");
		for (Result result : results) {
			System.out.println(HBaseDAOUtil.recoderToString(result));
		}

		hBaseDAO.close();
	}

}
