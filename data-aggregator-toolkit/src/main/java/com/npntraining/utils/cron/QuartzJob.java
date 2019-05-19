package com.npntraining.utils.cron;

import java.sql.SQLException;
import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.npntraining.utils.hive.HiveConnectionInstance;
import com.npntraining.watcher.FileWatcher;

public class QuartzJob implements Job {

	public void execute(JobExecutionContext context) throws JobExecutionException {

//		String load_time = <"static final value">;

		Date now = new Date();
		long ut3 = now.getTime() / 1000L;
		System.out.println("Linux stamp : " + ut3);
		System.out.println("Length : " + FileWatcher.TABLE_LIST.size());
		int loopCount = FileWatcher.TABLE_LIST.size();

//		for (String table : FileWatcher.TABLE_LIST) {
		for (int i = 0; i <= loopCount--;) {
			System.out.println("inside for loop " + loopCount);

			long l = Long.parseLong(FileWatcher.TABLE_LIST.get(loopCount));
			System.out.println("Long ime satmp : " + l + " Last table Entry : " + FileWatcher.LAST_TABLE_ENTRY);
//			System.out.println("boolean  : " + (l > Long.parseLong(FileWatcher.LAST_TABLE_ENTRY) && l <= ut3));
			if (l > Long.parseLong(FileWatcher.LAST_TABLE_ENTRY + "") && l <= ut3) {

				String sql = "INSERT OVERWRITE TABLE UsageTable3 PARTITION(loadTime='" + ut3
						+ "') SELECT load_time, report_time, aoperator, boperator, a_mobile_number, b_mobile_number, a_imsi, b_imsi, a_imei, b_imei, call_type, failure_type, latitude, longitude, call_start_time, call_end_time FROM UsageTable2 WHERE loadTime='"
						+ FileWatcher.TABLE_LIST.get(loopCount) + "'";
				System.out.println("=== ");
				System.out.println("Query : " + sql);

				try {
					HiveConnectionInstance.getsConnection().createStatement().execute(sql);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

//				FileWatcher.TABLE_LIST.remove(FileWatcher.TABLE_LIST.get(i));
				FileWatcher.LAST_TABLE_ENTRY = Long.parseLong(FileWatcher.TABLE_LIST.get(loopCount));
			}
		}

	}

}
