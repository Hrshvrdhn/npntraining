package Deepak.HiveConnectAndCRUDOperation;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DemoForCreatingDirectoryInHDFS extends Configured implements Tool{
	
		 
	private static final String hDFSuri = "hdfs://localhost.localdomain:9000/";	 
	static String dropFolderLcoalPath = "./Desktop/Deepak";	
	private static final Configuration config = new Configuration();	
	
	public int run(String[] args) throws Exception {
		
			
			String newDirectoryName = args[1].substring(0, args[1].lastIndexOf('.'));			
			
			String hdfsDropDirectoryPath = hDFSuri + newDirectoryName;			
			
			FileSystem fs = FileSystem.get(config);			
			
			//The HDFS Directory to be Created.
			Path pt = new Path(hdfsDropDirectoryPath);
			
			//Created a new Directory
			
			boolean isCreated = fs.mkdirs(pt);

			if (isCreated) {
			   System.out.println("Directory created");
			  }
			  		
			//Gets the Configuration Information of HDFS
			Configuration conf = getConf();		
		
			FileSystem fsForFileMovement = FileSystem.get(conf);
			Path pt1 = new Path(hdfsDropDirectoryPath + File.separator  +args[1]);
			OutputStream os = fsForFileMovement.create(pt1);	

		System.out.println("Step-1");
		
		//Data set is getting copied into input stream through buffer mechanism.
		//args[0] - Contains* the LFS + File Name from where needs to be copied to HDFS
		InputStream is = new BufferedInputStream(new FileInputStream(args[0]));
		
		System.out.println("Step-2");
		
		//Actual Copying of the dataset from Input Stream to Output Stream		
		IOUtils.copyBytes(is, os, conf);
		
		System.out.println("Moved the File : " + args[1] + " Successfully to HDFS.");
		
		//Set the Replication Factor
		fs.setReplication(pt, Short.parseShort("7"));
				
		//Move the File from the local Folder to Archieved Folder
		
		
		return 0;
		}

	/*
	 * Read the File Name and Create a Folder with 
	 * the Same Name in HDFS and Copy the file from the 
	 * Watch folder to the new HDFS Directory and archieve the file.
	 *  
	 */
	public static void CreateDirectory() throws Exception {

		//File Watcher API Creates a new Instance
		WatchService ws=FileSystems.getDefault().newWatchService();	
				
		//Path from where the File will be picked up(i.e LFS)
		java.nio.file.Path path1 =  Paths.get(dropFolderLcoalPath);
				
		//This folder on the LFS has to be watched continuously for the New Files ONLY. 
		path1.register(ws, StandardWatchEventKinds.ENTRY_CREATE);
				
		System.out.println("The Folder on the Local File System has been Started Polling.");
				
		WatchKey key;
				
				//
				while((key = ws.take())!=null)
				{
					for (WatchEvent<?> event : key.pollEvents())
					{				
						System.out.println("Event kind:"+event.kind()+
								". File Affected: /home/npntraining/Desktop/Deepak/"+event.context() + ".");
						
						String[] args1=new String[3];
						
						//Local File with File Path from where the File has to be picked up.
						//args1[0]="/home/npntraining/Desktop/Deepak/"+event.context();
						args1[0]= dropFolderLcoalPath +"/" + event.context();
						
						/*
						 * "Validation"
						 *  
						 *  Check only TXT FIle 
						 *  The Content is available inside the File
						 *  The File is already not available with the Same Name
						 *  
						
						*/
						
						//Just the File Name which needs to be moved to HDFS.
						args1[1]=event.context().toString();	
						
						//Setting the Replication Factor. 
						args1[2]="5";
						
						System.out.println("Moving File : " + event.context().toString() + " to HDFS.");
						
						ToolRunner.run(new DemoForCreatingDirectoryInHDFS(), args1);			
					}
					key.reset();
				}
				}	

				
		  
		 
		 public static void main(String args[]) throws Exception {
			 CreateDirectory();
			 }
}
