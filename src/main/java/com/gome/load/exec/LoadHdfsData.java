package com.gome.load.exec;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.gome.read.hdfs.HiveJdbcCli;

public class LoadHdfsData {
	static Connection con = null;
	static Statement stmt = null;
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, InterruptedException{
		String uri = "hdfs://10.58.50.66:9000/";  
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);
        String dt = args[0];
        FileStatus[] fliestatus = fs.listStatus(new Path("hdfs://10.58.50.66:9000/flume/logs/events/" + dt + "/"));
        int batch = 10;
        int count = 0;
        int batchNum = 0;
        con = HiveJdbcCli.getConn();
		stmt = con.createStatement();
		long begin = System.currentTimeMillis();
        for (FileStatus status : fliestatus) {
        	batchNum = count/batch;
        	Path path = status.getPath();
        	
        	String filepath = "hdfs://10.58.50.66:9000/flume/logs/events/" + dt + "/" + path.getName();
    		System.out.println(status.toString() +" : " + batchNum);
    		exec(filepath,String.valueOf(batchNum),dt);
        	//HiveJdbcCli.loadData(stmt, filepath, String.valueOf(batchNum),dt);
            count++;
        }
        long end = System.currentTimeMillis();
        System.out.println("=====load stage costs =======" + (end -begin) + " ms");
        stmt.close();
        con.close();
		
	    
	}
	public static void exec(String path,String batchNum,String dt) throws IOException, InterruptedException{
		System.out.println("python  E:\\code\\LoadHdfsData\\src\\com\\gome\\hdfs\\load\\py\\test.py " + path + " " + batchNum + " " + dt );
		Process process = Runtime.getRuntime().exec("python  E:\\code\\LoadHdfsData\\src\\com\\gome\\hdfs\\load\\py\\test.py " + path + " " + batchNum + " " + dt );
		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
	    BufferedReader br = new BufferedReader(isr);
	    String result = "";
	    String line = "";
		while(( line = br.readLine())!= null){
			result += line;
	        result += "\r\n";
		}
	    BufferedReader errbr = new BufferedReader(new InputStreamReader(new BufferedInputStream(process.getErrorStream())));
	    String error = "";
	    while ((error = errbr.readLine()) != null) {
	    	System.out.println("error :"+error);
	    }
	    process.waitFor();
	    process.destroy();
	    int exitValue = process.exitValue();
	    if(!result.equals("") || exitValue != 0){
	    	System.out.println("exec fail");
	    }else{
	    	System.out.println("exec success");
	    }
	}

}
