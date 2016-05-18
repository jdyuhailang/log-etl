package com.gome.hdfs.task.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateFormat {
	static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static String format(int number){
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, number);
		Date date = calendar.getTime();
		simpleDateFormat.format(date);
		return simpleDateFormat.format(date);
	}
	
}
