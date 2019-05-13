package com.nexenta.edgex.images;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import javax.xml.bind.DatatypeConverter;

import com.nexenta.edgex.EdgexClient;
import com.nexenta.edgex.util.CommonBase;

public class ImageLoader extends CommonBase {
	static EdgexClient edgex;

	int err = 0;
	String bucket;
	String object;
	ArrayList<String> keys;
	ArrayList<String> md5;

	public static String calcMD5(byte[] buf) {
		MessageDigest messageDigest;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.update(buf);
			byte[] digiest = messageDigest.digest();
			String hashedOutput = DatatypeConverter.printHexBinary(digiest);
			return hashedOutput;
		} catch (NoSuchAlgorithmException e) {
			return "";
		}
	}

	public ImageLoader(String bucket, String object) {
		this.bucket = bucket;
		this.object = object;
		out("\n\nSetup edgex client");
		String url = (System.getProperty("edgex") != null ? System.getProperty("edgex") : "http://localhost:9982");
		out("URL", url);
		String key = System.getProperty("key");
		String secret = System.getProperty("secret");
		String debug = System.getProperty("debug");
		out("key", key);
		out("secret", secret);

		this.keys = new ArrayList<String>();
		this.md5 = new ArrayList<String>();
		edgex = new EdgexClient(url,
				EdgexClient.DEFAULT_CONNECTION_TIMEOUT,
				EdgexClient.DEFAULT_READ_TIMEOUT,
				key,
				secret);
		if (debug != null)
			edgex.setDebugMode(2);
	}

	public int createObject() {
		this.createObject();
		out("\n\ncan create new empty KV object");
		return err;
	}

	public int loadByPath(String path) {
	    edgex.bucketCreate(bucket);
		int err = edgex.head(bucket, object);

		if (err == 404) { // Object not found
			err = edgex.keyValueCreate(bucket, object, "image/jpeg",
					EdgexClient.DEFAULT_CHUNKSIZE, EdgexClient.DEFAULT_BTREE_ORDER);
			if (err != 0) {
				out("Object create  err: ", err);
				return err;
			}
		}


		File fd = new File(path);
		if (!fd.exists())
			return -1;
		FileInputStream fi = null;
		int len, off, l;
		byte buf[];
		String m5;

		for (File f: fd.listFiles()) {
			if (f.isFile() && f.canRead()) {
				len = (int) f.length();
				out("Loading " + f.getAbsolutePath() + " len: " + len);
				fi = null;
				try {
					fi = new FileInputStream(f);
					buf = new byte[len];
					off = 0;
					while ((l = fi.read(buf, off, len)) >= 0 && len > 0) {
						off += l;
						len -= l;
					}

					m5 = calcMD5(buf);
					md5.add(m5);

					out(f.getName() + " insert md5: " + m5);
					err = edgex.keyValuePostOne(bucket, object, f.getName(), buf,
							"image/jpeg", f.lastModified(), true);
					if (err == 0) {
						keys.add(f.getName());
					}
				} catch (Exception e) {
					err("Loading error", e);
					return -2;
				} finally {
					try {	fi.close();	} catch (IOException ec) {}
				}
			}
		}
		// Close session
		err = edgex.keyValuePost(bucket, object, "",
				"",
				"text/csv", false);
		return 0;
	}

	public int testImages() {
		out("\n\nRead images by key");
		for (int i=0; i < keys.size(); i++) {
			String key = keys.get(i);
			out(key + " reading");
			int err = edgex.keyValueGet(bucket, object, key);
			if (err != 0) {
				error("Error reading image: " + key + " err: " + err);
				return -1;
			}
			byte[] arr = edgex.getByteResponse();
			String m = calcMD5(arr);
			out(key + " res length: "+ arr.length + " md5: " + m);
			if (!md5.get(i).equals(m)) {
				error(key + " md5 don't match: " + md5.get(i) +" - "  + m);
				return -2;
			}
		}
		return 0;
	}

	public void close() throws Exception {
		edgex.close();
	}

	public static void main(String[] args)  {
		if (args.length < 3) {
		   out("Usage: java -Dedgex=http://<s3 gateway ip>:<port> -Dkey=<s3 key> -Dsecret=<s3 secret key> ImageLoader <bucket> <image object> <data path>");
		   System.exit(1);
		}
		String bucket = args[0];
		String object = args[1];
		String path = args[2];

		ImageLoader loader = new ImageLoader(bucket, object);

		int err = loader.loadByPath(path);
		if (err != 0) {
			error(" Image load error: " + err);
			System.exit(1);
		}

		err = loader.testImages();
		if (err != 0) {
			error(" Image test error: " + err);
		}

		try {
			loader.close();
		} catch (Exception e) {
		}

	}

}
