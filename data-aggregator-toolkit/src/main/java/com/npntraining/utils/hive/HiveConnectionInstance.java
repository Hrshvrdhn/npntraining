package com.npntraining.utils.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.npntraining.constants.ConfigKeyConstants;

public class HiveConnectionInstance implements ConfigKeyConstants {

	private static Connection CONN = null;

	static {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private HiveConnectionInstance() {
	}

	public void fetchConnection() throws SQLException, ClassNotFoundException {
		if (CONN == null) {
			System.out.println("fetching connection instance..");
			CONN = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "npntraining", "npntraining");

		}
	}

	public static Connection getsConnection() throws SQLException, ClassNotFoundException {
		new HiveConnectionInstance().fetchConnection();
		return CONN;
	}
}