package com.nexenta.edgex;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.SignatureException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.nio.charset.StandardCharsets;

import com.nexenta.edgex.util.CommonBase;
import com.nexenta.edgex.util.ConvertUtil;

public class S3Signature extends CommonBase {
	private static Base64.Encoder encoder64 = Base64.getEncoder();
    private static String qstrings =
            ",acl,torrent,logging,location,policy,requestPayment,versioning" +
            "versions,versionId,notification,uploadId,uploads,partNumber,website" +
            "delete,lifecycle,tagging,cors,restore,replication,accelerate" +
            "inventory,analytics,metrics" +
            "response-cache-control,response-content-disposition,response-content-encoding" +
            "response-content-language,response-content-type,response-expires,";

    private int debugMode = 0;


	public S3Signature(int debugMode) {
		super();
		this.debugMode = debugMode;
	}

	public String signRequest(String secret, String method,
			String contentType,
			String date,
			String md5,
			URL url, Map<String, List<String>>headers) throws Exception {

		SortedMap<String, String>sortedHeaders = new TreeMap<String, String>();
		if (headers != null) {
			for (String key: headers.keySet()) {
				if (key == null)
					continue;
				List<String>value = headers.get(key);
				if (value == null) {
					sortedHeaders.put(key.toLowerCase(), "");
				} else {
					sortedHeaders.put(key.toLowerCase(), ConvertUtil.listToString(value));
				}
			}
		}
		String ssig = stringToSign(method,
     			contentType, date, md5,
    			url, sortedHeaders);
		 if (debugMode > 1)
			 out(ssig);
         return calcSignature(secret, ssig);
	}

	public void setDebugMode(int d) {
		debugMode = d;
	}

	private static SortedMap<String, String> queryMap(URL url)  {
		SortedMap<String, String> map = new TreeMap<String, String>();
	    String query = url.getQuery();
	    if (query == null)
	    	return map;
	    String[] pairs = query.split("&");
	    for (String pair : pairs) {
	        int idx = pair.indexOf("=");
	        if (idx < 0)
		        map.put(pair, "");
	        else
	             map.put(pair.substring(0, idx), pair.substring(idx + 1));
	    }
	    return map;
	}

	private static String stringToSign(String method,
			String contentType,
			String date, String md5,
			URL url, SortedMap<String, String>headers) {


            String out = "";
	        out += method + "\n";

	        // Add md5
	        out += (md5 != null ? md5 : "") + "\n";

	        // Add contentType
	        out += (contentType != null ? contentType.toLowerCase() : "") + "\n";

	        // Add date
	        if (headers != null) {
	        	if (headers.get("x-amz-date") != null) {
	        		date = headers.get("x-amz-date");
	        	}
	        	if (headers.get("expires") != null) {
	        		date = headers.get("expires");
	        	}
	        }
	        out += date + "\n";

	        // Add amz headers
	        if (headers != null) {
		        for (String hkey: headers.keySet()) {
		        	String h = hkey.toLowerCase();
		        	if (h.startsWith("x-amz-") && !h.startsWith("x-amz-date")) {
		        		out += h + ":" + headers.get(hkey) + "\n";
		        	}
		        }
	        }

	        // canonical request
	        String path = url.getPath();

	        out += (!path.equals("") ? path : '/');
	        SortedMap<String, String>qmap = queryMap(url);
	        Set<String>keys = qmap.keySet();
	        int n = 0;
	        String query = "";
	        for (String key: keys) {
	        	if (qstrings.indexOf("," + key + ",") < 0)
	        		continue;
	            if (n > 0)
	            	query += "&";
	        	n++;
	        	String value = qmap.get(key);
	        	if (!value.equals(""))
	                query += key + "=" + value;
	        	else
	                query += key;
	        }
	        if (!query.equals("")) {
	        	out += "?" + query;
	        }

	        return out;
	   }


	private static String calcSignature(String secret, String stringToSign) throws Exception {
		SecretKeySpec signingKey = new SecretKeySpec(secret.getBytes(), "HmacSHA1");
		Mac mac = Mac.getInstance("HmacSHA1");
		mac.init(signingKey);
		byte[] sig1 = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
		//out(javax.xml.bind.DatatypeConverter.printHexBinary(sig1));
	    return new String(encoder64.encode(sig1));
	}



}
