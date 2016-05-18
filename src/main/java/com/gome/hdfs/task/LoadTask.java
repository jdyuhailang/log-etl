package com.gome.hdfs.task;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gome.hdfs.task.utils.DateFormat;
import com.gome.presto.PrestoJdbcCli;
import com.gome.read.hdfs.HiveJdbcCli;

public class LoadTask extends TimerTask {
	static Logger logger = LoggerFactory.getLogger(LoadTask.class);
	static Connection con = null;
	static Statement stmt = null;
	private FileSystem fs;
	public LoadTask(FileSystem fs){
		this.fs = fs;
	}
	@Override
	public void run() {
		logger.info("LoadTask run...");
		logger.info("begin export data");
		try {
			String dt = DateFormat.format(-1);
	        FileStatus[] fliestatus = fs.listStatus(new Path("hdfs://10.58.50.66:9000/flume/logs/events/" + dt + "/"));
	        
	        int batch = 10;
	        int count = 0;
	        List<String> paths = new ArrayList<String>();
	        int batchNum = 0;
	        getConnection();
			long begin = System.currentTimeMillis();
	        for (FileStatus status : fliestatus) {
	        	batchNum = count/batch;
	        	Path path = status.getPath();
	        	
	        	String filepath = "hdfs://10.58.50.66:9000/flume/logs/events/" + dt + "/" + path.getName();
	    		paths.add(filepath);
	    		logger.info(status.toString() +" : " + batchNum);
	        	HiveJdbcCli.loadData(stmt, filepath, String.valueOf(batchNum),dt);
	            count++;
	        }
	        long end = System.currentTimeMillis();
	        logger.info("=====load stage costs =======" + (end -begin) + " ms");
	        close();
	        FileStatus[] warehouse = fs.listStatus(new Path("hdfs://10.58.50.66:9000/user/hive/warehouse/gm_cloud_logs.db/test/dt=" + dt + "/"));
	        begin = System.currentTimeMillis();
	        for(FileStatus status : warehouse){
	        	getConnection();
	        	Path path = status.getPath();
	        	String batchName = path.getName();
	        	HiveJdbcCli.insertOrc(stmt,batchName,dt);
	        	close();
	        }
	        end = System.currentTimeMillis();
	        logger.info("=====insert table logs stage costs =======" + (end -begin) + " ms");
	        List<String>  apps = PrestoJdbcCli.getApplication(dt);
	        begin = System.currentTimeMillis();
	        for(String app:apps){
	        	getConnection();
				HiveJdbcCli.insertOrc(stmt,"",app,dt);
				close();
			}
	        end = System.currentTimeMillis();
	        logger.info("=====insert table glogs stage costs =======" + (end -begin) + " ms");
	        logger.info("success export data");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
        
	}
	public void getConnection(){
		try {
			con = HiveJdbcCli.getConn();
			stmt = con.createStatement();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void close(){
		try {
			stmt.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
