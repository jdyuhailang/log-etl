package com.gome.read.hdfs;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gome.presto.PrestoJdbcCli;


public class HdfsToHive {
	static Connection con = null;
	static Statement stmt = null;
	static Logger logger = LoggerFactory.getLogger(HdfsToHive.class);
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		logger.info("begin export data");
		String uri = "hdfs://10.58.50.66:9000/";  
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);
        String dt = args[0];
        FileStatus[] fliestatus = fs.listStatus(new Path("hdfs://10.58.50.66:9000/flume/logs/events/" + dt + "/"));
        
        //LzoIndexer lzoIndexer1 = new LzoIndexer(config);
        //lzoIndexer1.index(new Path("hdfs://10.58.50.66:9000/flume/logs/events/2016-04-26/gm_cloud_logs_10.58.44.95.1461662074372.lzo"));
        
        int batch = 10;
        int count = 0;
        List<String> paths = new ArrayList<String>();
        int batchNum = 0;
        con = HiveJdbcCli.getConn();
		stmt = con.createStatement();
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
        stmt.close();
        con.close();
        FileStatus[] warehouse = fs.listStatus(new Path("hdfs://10.58.50.66:9000/user/hive/warehouse/gm_cloud_logs.db/test/dt=" + dt + "/"));
        begin = System.currentTimeMillis();
        for(FileStatus status : warehouse){
        	con = HiveJdbcCli.getConn();
    		stmt = con.createStatement();
        	Path path = status.getPath();
        	String batchName = path.getName();
        	HiveJdbcCli.insertOrc(stmt,batchName,dt);
        	stmt.close();
            con.close();
        }
        end = System.currentTimeMillis();
        logger.info("=====insert table logs stage costs =======" + (end -begin) + " ms");
        List<String>  apps = PrestoJdbcCli.getApplication(dt);
        begin = System.currentTimeMillis();
        for(String app:apps){
        	con = HiveJdbcCli.getConn();
    		stmt = con.createStatement();
			HiveJdbcCli.insertOrc(stmt,"",app,dt);
			stmt.close();
	        con.close();
		}
        end = System.currentTimeMillis();
        logger.info("=====insert table glogs stage costs =======" + (end -begin) + " ms");
        logger.info("success export data");
	}

}
