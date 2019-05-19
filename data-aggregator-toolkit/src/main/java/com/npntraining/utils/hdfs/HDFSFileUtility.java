package com.npntraining.utils.hdfs;

import java.nio.file.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.WatchService;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.npntraining.connections.HiveConection;

public class HDFSFileUtility {
	private static final String hDFSuri = "hdfs://localhost:9000";
	private static final Configuration config = new Configuration();

	private void createDirectory(String hdfsDirectory) throws IOException, ClassNotFoundException, SQLException {
		FileSystem fs = FileSystem.get(URI.create(hDFSuri), config);
		Path pt = new Path(hdfsDirectory);
		boolean isCreated = fs.mkdirs(pt);
		if (isCreated) {
			System.out.println("Directory created & name of the directory is : " + hdfsDirectory);
		}
	}

	public void copyFromLFS(String localFSPath, String hdfsPath)
			throws ClassNotFoundException, SQLException, IOException {
		String arguments = hDFSuri + File.separator + hdfsPath;
		createDirectory(arguments);
		FileSystem fs = FileSystem.get(URI.create(hDFSuri), config);
		fs.copyFromLocalFile(new Path(localFSPath), new Path(arguments));
	}

}
