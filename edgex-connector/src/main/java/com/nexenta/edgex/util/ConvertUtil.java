package com.nexenta.edgex.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
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


	public static ByteBuffer[] strToBuffer(String ...arr) {
		ByteBuffer res[] = new ByteBuffer[arr.length];
		for (int i = 0; i < arr.length; i++) {
			res[i] = ByteBuffer.allocate(arr[i].length());
			res[i].put(arr[i].getBytes(Charset.forName("UTF-8")));
		}
		return res;
	}

}
