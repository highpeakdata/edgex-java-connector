package com.nexenta.edgex.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class FormatUtil {

	public static String formatedTime(Date date) {
		String res = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
		return res;
	}

	public static String formatResult(Result result) {
		StringBuilder buf = new StringBuilder();
		buf.append(String.format("Test run: %d, Failed: %s",
				result.getRunCount(), result.getFailureCount()));
		buf.append("\n");
		if (result.getFailureCount() > 0) {
			buf.append("Failures:\n");
			for (Failure f : result.getFailures()) {
				buf.append(f.toString());
				buf.append("\n");
			}
		}
		return buf.toString();
	}


	public static String stackTraceToString(Exception e) {
		StringWriter stringWriter = new StringWriter();
		e.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}
}
