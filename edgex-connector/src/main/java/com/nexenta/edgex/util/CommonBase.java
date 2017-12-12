package com.nexenta.edgex.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class CommonBase {
	public static int details = 0;

	public CommonBase() {
		super();
	}

	private static String prefix() {
		if (details == 0)
			return "";
		String location = "";
		StackTraceElement[] etrace = Thread.currentThread().getStackTrace();
		if (etrace.length > 3) {
			String fullName = etrace[3].getClassName();
			location = String.format("%s:%3d,",
					fullName.substring(fullName.lastIndexOf(".") + 1),
					etrace[3].getLineNumber());
		}
		return String.format("%s [%s%s] ",
				FormatUtil.formatedTime(new Date()),
				location, Thread.currentThread().getName());
	}

	public static void out(String s) {
		System.out.println(prefix() + s);
	}

	public static void out(String s, int n) {
		System.out.println(prefix() + s + ": " + n);
	}

	public static void out(String s, String n) {
		System.out.println(prefix() + s + ": " + n);
	}


	public static void out(int n, String s) {
		System.out.println(String.join("", Collections.nCopies(n, "\n")));
		out(s);
	}

	public static void err(String s, Exception e) {
		System.err.println(prefix() + s + "\n" + FormatUtil.stackTraceToString(e));
	}

	public static void out(String s, Map<String, String> map) {
		if (map == null) {
		   out(s + ": ");
		   return;
		}
		StringBuilder tmp = new StringBuilder();
		for (String key: map.keySet()) {
			tmp.append("  ");
			tmp.append(key);
			tmp.append(": ");
			Object value = map.get(key);
			if (value != null)
				tmp.append(value.toString());
			tmp.append("\n");
		}
        out(s + ":\n" + tmp.toString());
	}

	public static void debug(String s, Map<String, String> map) {
		if (map == null) {
		   out(s + ": ");
		   return;
		}
		StringBuilder tmp = new StringBuilder();
		for (String key: map.keySet()) {
			tmp.append("  ");
			tmp.append(key);
			tmp.append(": ");
			Object value = map.get(key);
			if (value != null)
				tmp.append(value.toString());
			tmp.append("\n");
		}
        debug(s + ":\n" + tmp.toString());
	}

	public static void debug(String h, String s) {
		System.out.println(h + ": " + s);
	}

	public static void debug(String h) {
		System.out.println(h);
	}

	public static void debug(String h, int s) {
		System.out.println(h + ": " + s);
	}

	public static void debug(String h, long s) {
		System.out.println(h + ": " + s);
	}

	public static void debug(String h, boolean s) {
		System.out.println(h + ": " + s);
	}

	public static void error(String s) {
		System.err.println(s);
	}

	public static void error(String h, Exception e) {
		error("Error: " + h + " - " + stackTraceToString(e));
	}

	public static String stackTraceToString(Exception e) {
		if (e == null)
			return "";
		StringWriter stringWriter = new StringWriter();
		e.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}

}


