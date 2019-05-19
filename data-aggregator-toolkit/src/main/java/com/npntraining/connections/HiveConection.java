package com.npntraining.connections;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class HiveConection {


		public static Connection connectToDB() throws ClassNotFoundException, SQLException, IOException {
			
			File file = new File("/home/npntraining/Desktop/Practice-Files/Hadoop/workspace/data-aggregator-toolkit/src/resources/configure.properties");
			InputStream inputfile = new FileInputStream(file);
			Properties prop = new Properties();
			prop.load(inputfile);
			
			String url = prop.getProperty("url");
			String drivername = prop.getProperty("driverName");
			String username=prop.getProperty("username");
			String password=prop.getProperty("password");
			
			Class.forName(drivername);
			System.out.println("Driver Loaded successfully");
			Connection con = DriverManager.getConnection(
					url, username,
					password);
			System.out.println("Obtained Connection Object");
			return con;
	}
/*	
	public static void main(String Args[]) throws ClassNotFoundException, SQLException, IOException{
		HiveConection hv= new HiveConection();
		hv.connectToDB();
	}
	
	*/
}
