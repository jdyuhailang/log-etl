package com.gome.log.etl;

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gome.hdfs.task.LoadTask;

public class LogTimer {
	static Logger logger = LoggerFactory.getLogger(LogTimer.class);
	public static final long period = 1000 * 60 * 60 * 24;
	public static void main(String[] args) throws IOException {
		logger.info("LogTimer start...");
		Timer timer = new Timer();
		Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        //定制每天的18:00:00执行，
        calendar.set(year, month, day+1, 00, 40, 00);
        Date date = calendar.getTime();
        String uri = "hdfs://10.58.50.66:9000/";  
        Configuration config = new Configuration();
        FileSystem fs;
        fs = FileSystem.get(URI.create(uri), config);
        LoadTask task = new LoadTask(fs);
        timer.schedule(task, date, period);
	}

}
