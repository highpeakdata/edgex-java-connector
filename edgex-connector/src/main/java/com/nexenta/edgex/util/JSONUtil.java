package com.nexenta.edgex.util;

import java.io.IOException;
import java.io.StringReader;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JSONUtil {

	public static JSONObject parseJSON(String s) {
		Object obj;
		try {
			obj = new JSONParser().parse(new StringReader(s));
		} catch (IOException | ParseException e) {
			return null;
		}

        JSONObject jo = (JSONObject) obj;
        return jo;
	}

	public static int sizeJSON(String s) {
		Object obj;
		try {
			obj = new JSONParser().parse(new StringReader(s));
		} catch (IOException | ParseException e) {
			return 0;
		}

		if (obj instanceof JSONObject) {
	        JSONObject jo = (JSONObject) obj;
	        return jo.entrySet().size();
		}

		if (obj instanceof JSONArray) {
			JSONArray ja = (JSONArray) obj;
	        return ja.size();
		}

		return 0;
	}

}
