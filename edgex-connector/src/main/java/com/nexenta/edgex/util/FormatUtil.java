package com.nexenta.edgex.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FormatUtil {

	public static String formatedTime(Date date) {
		String res = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
		return res;
	}

	public static String stackTraceToString(Exception e) {
		StringWriter stringWriter = new StringWriter();
		e.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}
}
