package com.gome.read.hdfs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveJdbcCli {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://10.58.50.66:10000/gm_cloud_logs";  
    private static String user = "hive";  
    private static String password = "hive";  
    private static String sql = "";  	
    private static ResultSet res;  
    static Logger logger = LoggerFactory.getLogger(HiveJdbcCli.class);
	public static void countData(Statement stmt, String tableName) throws SQLException{  
        sql = "select count(1) from " + tableName;  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
        System.out.println("执行“regular hive query”运行结果:");  
        while (res.next()) {  
            System.out.println("count ------>" + res.getString(1));  
        }  
    }  
    public static void selectData(Statement stmt, String tableName)  
            throws SQLException {  
        sql = "select * from " + tableName;  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
        System.out.println("执行 select * query 运行结果:");  
        while (res.next()) {  
            System.out.println(res.getInt(1) + "\t" + res.getString(2));  
        }  
    }  
    public static void loadData(Statement stmt,String filepath, String batch,String dt)  
            throws SQLException {  
        /*sql = "load data local inpath '" + filepath + "' into table "  
                + tableName;  */
        sql = "LOAD DATA INPATH '" + filepath + "' into table test PARTITION(dt='" + dt + "',batch='" + batch +  "')"  ;
        //sql = "LOAD DATA INPATH '/flume/logs/events/2016-04-28/gm_cloud_logs_10.58.44.95.1461772800692.lzo' into table test PARTITION(dt='2016-04-28',batch='0')";
    	logger.info("Running:" + sql);
        stmt.execute(sql);  
    }  
    public static void insertOrc(Statement stmt,String batchName,String dt)  
            throws SQLException {  
        /*sql = "load data local inpath '" + filepath + "' into table "  
                + tableName;  */
        sql = "INSERT into TABLE logs partition (dt='" + dt + "') "
        		+ "select  str_to_map(regexp_replace(substr(split(message,'}')[0],2),', ',','),',','=')['timestamp'],"
        		+ "str_to_map(regexp_replace(substr(split(message,'}')[0],2),', ',','),',','=')['application'],"
        		+ "str_to_map(regexp_replace(substr(split(message,'}')[0],2),', ',','),',','=')['sequence'],"
        		+ "str_to_map(regexp_replace(substr(split(message,'}')[0],2),', ',','),',','=')['filename'],"
        		+ "str_to_map(regexp_replace(substr(split(message,'}')[0],2),', ',','),',','=')['ip'],"
        		+ "substr(message,instr(message,'}') +2) from test where dt='" + dt + "' and " + batchName ;
        System.out.println("Running:" + sql);  
        logger.info("Running:" + sql);
        stmt.execute(sql);  
    }  
    public static void insertOrc(Statement stmt,String batchName,String application,String dt)  
            throws SQLException {  
        /*sql = "load data local inpath '" + filepath + "' into table "  
                + tableName;  */
        sql = "INSERT into TABLE t1 partition (dt='" + dt + "' , application='" + application + "') "
        		+ "select  ts,sequence,filename,ip,message from logs where dt='" + dt + "'  and application='" + application + "'";
        logger.info("Running:" + sql);
        stmt.execute(sql);  
    }  
    public static void describeTables(Statement stmt, String tableName)  
            throws SQLException {  
        sql = "describe " + tableName;  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
        System.out.println("执行 describe table 运行结果:");  
        while (res.next()) {  
            System.out.println(res.getString(1) + "\t" + res.getString(2));  
        }  
    }  
    public static void showTables(Statement stmt, String tableName)  
            throws SQLException {  
        sql = "show tables '" + tableName + "'";  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
        System.out.println("执行 show tables 运行结果:");  
        if (res.next()) {  
            System.out.println(res.getString(1));  
        }  
    }  
    public static void createTable(Statement stmt, String tableName)  
            throws SQLException {  
        sql = "create table "  
                + tableName  
                + " (key int, value string)  row format delimited fields terminated by '\t'";  
        stmt.executeQuery(sql);  
    }
    public static String dropTable(Statement stmt) throws SQLException {  
        // 创建的表名  
        String tableName = "testHive";  
        sql = "drop table " + tableName;  
        stmt.executeQuery(sql);  
        return tableName;  
    }  
    public static Connection getConn() throws ClassNotFoundException,  
            SQLException {
        Class.forName(driverName);  
        Connection conn = DriverManager.getConnection(url, user, password);  
        return conn;  
    }
}
