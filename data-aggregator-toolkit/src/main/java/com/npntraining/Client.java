package com.npntraining;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

import org.quartz.SchedulerException;

import com.npntraining.constants.FileLocationConstants;
import com.npntraining.utils.cron.QuartzMain;
import com.npntraining.watcher.FileWatcher;

public class Client {
	static FileWatcher fileWatcher;
//	QuartzMain qrtz;

	public static void main(String[] args) {
		fileWatcher = new FileWatcher();
		try {
			fileWatcher.watchForFiles(FileLocationConstants.INPUT_DIRECTORY_SCANNER);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		Date now = new Date();
//		long ut3 = now.getTime() / 1000L;
//		System.out.println(ut3>(ut3+10));
//		System.out.println(ut3+10);

	}

}
