package com.nexenta.edgex.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConvertUtil {

	public static String strToJson(Object ...arr) {
		StringBuffer res = new StringBuffer("{");
		int n=0;
		for (int i = 0; i < arr.length; i += 2) {
			if (n > 0)
				res.append(", ");
			res.append(" \"");
			res.append(arr[i].toString());
			res.append("\": \"");
			res.append(arr[i+1].toString());
			res.append("\"");
			n++;
		}
		res.append(" }");
		return res.toString();
	}


	public static Map<String,String> strToMap(Object ...arr) {
		Map<String,String>res = new HashMap<String, String>();
		for (int i = 0; i < arr.length; i += 2) {
			res.put(arr[i]+"", arr[i+1]+"");
		}
		return res;
	}


	public static Map<String,List<String>> mapToListMap(Map<String,String> map) {
		Map<String,List<String>>res = new HashMap<String, List<String>>();
		for (String key: map.keySet()) {
			List<String>list = res.get(key);
			if (list == null) {
				list = new ArrayList<String>();
				res.put(key, list);
			}
			String value = map.get(key);
			list.add(value != null ? value : "");
		}
		return res;
	}

	public static Map<String,List<String>> arrayToListMap(Object arr[]) {
		Map<String,List<String>>res = new HashMap<String, List<String>>();
		for (int i = 0; i < arr.length; i += 2) {
			List<String>list = res.get(arr[i]);
			if (list == null) {
				list = new ArrayList<String>();
				res.put(arr[i]+"", list);
			}
			list.add(arr[i+1]+"");
		}
		return res;
	}

	public static Map<String,List<String>> strToListMap(Object ...arr) {
		return arrayToListMap(arr);
	}

	public static ByteBuffer[] strToBuffer(String ...arr) {
		ByteBuffer res[] = new ByteBuffer[arr.length];
		for (int i = 0; i < arr.length; i++) {
			res[i] = ByteBuffer.allocate(arr[i].length());
			res[i].put(arr[i].getBytes(Charset.forName("UTF-8")));
		}
		return res;
	}

	public static String listToString(List<String>list) {
		if (list == null)
			return "";
		StringBuilder res = new StringBuilder();
		for (String s: list) {
			if (res.length() > 0)
				res.append(',');
			res.append(s);
		}
		return res.toString();
	}

}
