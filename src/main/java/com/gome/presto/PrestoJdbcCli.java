package com.gome.presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class PrestoJdbcCli {
	private static Statement statement;
	private static Connection conn;
	static Logger logger = LoggerFactory.getLogger(PrestoJdbcCli.class);
	public static void main(String[] args) throws Exception {
		/*List<String>  apps = getApplication("2016-05-01");
		for(String app:apps){
			System.out.println(app);
		}*/
		//String sql="select * from glogs where dt=#{dt} and application=#{application} order by sequence asc limit 10";
		String sql = "select * from glogs where # order by sequence asc limit 10";
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("dt", "2016-05-03");
		map.put("application", "t3_log");
		//map.put("ip", "10.58.173.83");
		//map.put("filename", "/app/stage/pangu-trading_01/logs/app.log");
		//map.put("ts", "1462118400000,1462122000000");
		//map.put("message", "ERROR");
		presto_query(sql,map);
	}
	public static Connection createConnection()
            throws SQLException, ClassNotFoundException
    {
		//Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        return DriverManager.getConnection("jdbc:presto://10.58.50.249:5050/hive/gm_cloud_logs", "rw_cld_hive_dev", "lK7mN6GtZZDj");
    }
	
	public static List<String> getApplication(String dt) throws ClassNotFoundException, SQLException{
		List<String> apps = new ArrayList<String>();
		
		conn = createConnection();
		statement = conn.createStatement();
		ResultSet rs = statement.executeQuery("select application,count(1) as count from logs where dt='" + dt + "' group by application") ;
		
		/*ResultSetMetaData metadata = rs.getMetaData();
		
		System.out.println(metadata.getColumnCount());*/
		while(rs.next()){
			if(rs.getString("application") != null && !"null".equals(rs.getString("application")))
				apps.add(rs.getString("application"));
		}
		return apps;
	}
	public static void query(String sql,Map<String,Object> map) throws SQLException, ClassNotFoundException{
		conn = createConnection();
		statement = conn.createStatement();
		for(String key : map.keySet()){
			sql = sql.replace("#{" + key + "}", "'" + (String) map.get(key) + "'");
		}
		System.out.println(sql);
		long begin = System.currentTimeMillis();
		ResultSet rs = statement.executeQuery(sql) ;
		
		ResultSetMetaData metadata = rs.getMetaData();
		
		System.out.println(metadata.getColumnCount());
		long end = System.currentTimeMillis();
		System.out.println((end - begin) + " ms ");
		
		while(rs.next()){
			System.out.println(rs.getString("ip")+ "," + rs.getString("message"));
		}
		statement.close();
		conn.close();
	}
	public static void presto_query(String sql,Map<String,Object> map) throws SQLException, ClassNotFoundException{
		conn = createConnection();
		statement = conn.createStatement();
		
		String where = "";
		for(String key : map.keySet()){
			if(key.equals("ts")){
				String ts = (String) map.get(key);
				where += "ts between '" + ts.split(",")[0] + "' and '" + ts.split(",")[1] + "' and ";
			}else if (key.equals("message")){
				where += key + " like " + "'%" + (String) map.get(key) + "%' and ";
			}else{
				where += key + "=" + "'" + (String) map.get(key) + "' and ";
			}
		}
		if(!where.equals(""))
			where = where.substring(0, where.length()-4);
		System.out.println(where);
		sql = sql.replace("#", where);
		System.out.println(sql);
		long begin = System.currentTimeMillis();
		ResultSet rs = statement.executeQuery(sql) ;
		
		ResultSetMetaData metadata = rs.getMetaData();
		
		System.out.println(metadata.getColumnCount());
		long end = System.currentTimeMillis();
		System.out.println((end - begin) + " ms ");
		
		while(rs.next()){
			System.out.println(rs.getString("dt")+ "," + rs.getString("message") + ", " + rs.getString("filename"));
			//System.out.println(rs.getInt(1));
		}
		statement.close();
		conn.close();
	}
}
