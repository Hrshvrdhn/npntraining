package com.npntraining.watcher;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.quartz.SchedulerException;

import com.npntraining.utils.hive.HiveUtils;
import com.npntraining.utils.cron.QuartzMain;
import com.npntraining.utils.hdfs.HDFSFileUtility;

public class FileWatcher {

	public static List<String> TABLE_LIST = new ArrayList<String>();
	public static long LAST_TABLE_ENTRY = 0;

	HDFSFileUtility hdfsFileUtility = new HDFSFileUtility();

	public void watchForFiles(String directory)
			throws IOException, InterruptedException, ClassNotFoundException, SQLException, SchedulerException {
		QuartzMain.startScheduling();
		WatchService watchService = FileSystems.getDefault().newWatchService();

		Path path = Paths.get(directory);

		WatchKey key = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

		while ((key = watchService.take()) != null) {
			for (WatchEvent<?> event : key.pollEvents()) {
				System.out.println("event");
				String FileNameInLFS = event.context().toString();
				System.out.println(FileNameInLFS);
				TABLE_LIST.add(FileNameInLFS);
				System.out.println("tabl list : " + TABLE_LIST);
//				String actualDirectory = FileNameInLFS.substring(0, FileNameInLFS.lastIndexOf("."));
				// hdfsFileUtility.copyFromLFS(directory + "/" + FileNameInLFS,
				// actualDirectory);

				new HiveUtils().createPartitionTable(FileNameInLFS);

			}
			key.reset();
		}

	}
}
