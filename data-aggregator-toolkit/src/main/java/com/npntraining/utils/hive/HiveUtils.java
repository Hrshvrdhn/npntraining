package com.npntraining.utils.hive;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.npntraining.utils.hive.HiveConnectionInstance;
//import com.npntraining.constants.ConfigKeyConstants;

public class HiveUtils {

	public void createPartitionTable(String file_name) throws ClassNotFoundException, SQLException {

//		createUsageTable();

		Connection cinn = HiveConnectionInstance.getsConnection();
		Statement stm = cinn.createStatement();
		String sql = "LOAD DATA LOCAL INPATH '/home/npntraining/eclipse-workspace/data/current/" + file_name + "' OVERWRITE INTO TABLE "
				+ "UsageTable2 " + "PARTITION(loadTime='" + file_name + "')";
		System.out.println("load cmd :" + sql);
		stm.execute(sql);
		System.out.println("Loaded data into the table successfuly");
	}

	public void createUsageTable() throws ClassNotFoundException, SQLException {
		String CREATE_TABLE_QUERY = "CREATE TABLE UsageTable (load_time BIGINT, report_time BIGINT, aoperator STRING, Boperator STRING, A_Mobile_number BIGINT, B_Mobile_Number BIGINT, "
				+ "A_INSI BIGINT, B_IMSI BIGINT, A_IMEI BIGINT, B_IMEI BIGINT, call_type STRING, Failure_type Array<STRING>, latitude STRING, longitue STRING, city STRING, "
				+ "state STRING, call_start_time BIGINT, call_end_time BIGINT)"
				+ " PARTITIONED BY (loadTime STRING) ROW FORMAT DELIMITED " + "FIELDS TERMINATED BY ','"
				+ " COLLECTION ITEMS TERMINATED BY ' '" + " STORED AS TextFile;";

		System.out.println("SQL = " + CREATE_TABLE_QUERY);
		Statement stmt = HiveConnectionInstance.getsConnection().createStatement();
//		stmt.setString(1, file_name);
		stmt.execute(CREATE_TABLE_QUERY);
	}

}
