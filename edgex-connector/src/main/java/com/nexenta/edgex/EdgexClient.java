package com.nexenta.edgex;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nexenta.edgex.util.CommonBase;

/**
 * Edge-x java connector
 *
 */
public class EdgexClient extends CommonBase {
	static int SS_CONT = 0x00;
	static int SS_FIN = 0x01;
	static int SS_APPEND = 0x02;
	static int SS_RANDWR = 0x04;
	static int SS_KV = 0x08;
	static int SS_STAT = 0x10;
	static int CCOW_O_REPLACE = 0x01;
	static int CCOW_O_CREATE = 0x02;
	static int BYTE_BUFFER = 16*1024;

	static byte NL[] = { 10 };

	public static int DEFAULT_CHUNKSIZE = 4096;
	public static int DEFAULT_BTREE_ORDER = 4;

	public static int DEFAULT_CONNECTION_TIMEOUT = 60000; // 60 seconds
	public static int DEFAULT_READ_TIMEOUT = 600000; // 600 seconds

	String url;
	String path;
	String sid;
	HttpURLConnection con;
	int connectionTimeout;
	int readTimeout;
	long logicalSize;

	int responseCode;
	String errorMsg;
	ArrayList<String> response;
	ByteArrayOutputStream byteResponse;
	String lastFormat;
	int err;
	int debugMode;

	/**
	 * Edge-x client constructor
	 *
	 * @param url - s3 url. E.g. http://localhost:9982
	 * @param connectionTimeout - connection timeout in millisecconds
	 * @param readTimeout - read timeout in milliseconds
	 */
	public EdgexClient(String url, int connectionTimeout, int readTimeout) {
		super();
		this.url = url;
		this.path = "";
		this.sid = null;
		this.logicalSize = 0;
		this.connectionTimeout = connectionTimeout;
		this.readTimeout = readTimeout;
		this.responseCode = 0;
		this.errorMsg = null;
		this.lastFormat = null;

		this.err = 0;
		this.debugMode = 0;
	}

	/**
	 * Inner connection init method
	 * @param method - HTTP method
	 * @param needOutput: true output expected, false - not
	 * @return
	 */
	private int init(String method, boolean needOutput) {
		URL obj;
		this.err = 0;
		this.logicalSize = 0;
		try {
			obj = new URL(url + this.path);
		} catch (MalformedURLException e) {
			return createErrorMessage(1, "Invalid url: " + url, e);
		}
		try {
			con = (HttpURLConnection) obj.openConnection();
		} catch (IOException e) {
			return createErrorMessage(2, "Connection error url: " + url, e);
		}
		try {
			con.setConnectTimeout(this.connectionTimeout);
			con.setReadTimeout(this.readTimeout);
			con.setRequestMethod(method);
			if (needOutput)
				con.setDoOutput(true);
		} catch (Exception e) {
			return createErrorMessage(3, "Connect error3 url: " + url, e);
		}
		return 0;
	}

	/**
	 * Close Edge-x connection.
	 * Disconnect from Edge-x server
	 */
	public void close() {
		try {
			if (con != null)
				con.disconnect();
		} catch (Exception ee) {
		} finally {
			this.con = null;
		}
	}

	/**
	 * Get last error message
	 *
	 * @return error message string
	 */
	public String getErrorMsg() {
		return errorMsg;
	}

	/**
	 * Get last error code
	 *
	 * @return error code
	 */
	public int getErr() {
		return err;
	}

	/**
	 * Get last HTTP response code
	 *
	 * @return HTTP response code
	 */
	public int getResponseCode() {
		return responseCode;
	}

	/**
	 * Get logical size of object returned by last head call
	 *
	 * @return logical size
	 */
	public long getLogicalSize() {
		return logicalSize;
	}



	/**
	 * Get last list request format
	 * @return lastFormat as string
	 */
	public String getLastFormat() {
		return lastFormat;
	}

	/**
	 * Setup debug mode
	 *
	 * @param debugMode:
	 *   0 - no debug output,
	 *   1 - input debug,
	 *   2 - input and output  debug
	 */
	public void setDebugMode(int debugMode) {
		this.debugMode = debugMode;
	}

	/**
	 * Get response headers returned by last call
	 *
	 * @return response heades as Map
	 */
	public Map<String, String> getHeaders() {
		Map<String, List<String>> map = con.getHeaderFields();
		HashMap<String, String> res = new HashMap<String, String>(map.size());
		for (String key : map.keySet()) {
			List<String> list = map.get(key);
			StringBuilder tmp = new StringBuilder();
			int n = 0;
			if (list.size() == 1) {
				res.put(key, list.get(0));
				continue;
			}
			if (list.size() == 0) {
				res.put(key, "");
				continue;
			}
			for (String one : list) {
				if (n > 0)
					tmp.append(";");
				tmp.append(one);
				n++;
			}
			res.put(key, tmp.toString());
		}
		return res;
	}

	/**
	 * Inner method to do object read
	 *
	 * @param mode - read mode
	 * @param binary - binary read
	 * @return - error code
	 */
	private int read(int mode, boolean binary) {
		try {
			this.responseCode = con.getResponseCode();
			if (this.debugMode > 1) {
				debug("responseCode", responseCode);
				debug("Headers", this.getHeaders());
			}
			if (this.responseCode >= 300) {
				this.close();
				return createErrorMessage(this.responseCode, "Server error url: " + this.url, null);
			}
			if (mode == SS_CONT) {
				this.sid = con.getHeaderField("x-session-id");
				if (this.debugMode > 1) {
					debug("sid", sid);
				}
			} else {
				this.sid = null;
			}
			if (con.getHeaderField("x-ccow-logical-size") != null) {
				this.logicalSize = con.getHeaderFieldLong("x-ccow-logical-size", 0);
				if (this.debugMode > 1) {
					debug("logicalSize", logicalSize);
				}
			}

			if (binary) {
				this.response = null;
				this.byteResponse = new ByteArrayOutputStream(BYTE_BUFFER);
				if (con.getContentLength() > 0) {
					InputStream in = con.getInputStream();
					byte b[] = new byte[BYTE_BUFFER];
					int len;
					while ((len = in.read(b)) > 0) {
						this.byteResponse.write(b, 0, len);
					}
					in.close();
					if (this.debugMode > 1) {
						debug("Response:\n" + this.byteResponse.size());
					}
				}
			} else { // text input
				this.byteResponse = null;
				this.response = new ArrayList<String>();
				if (con.getContentLength() > 0) {
					BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
					String inputLine;
					while ((inputLine = in.readLine()) != null) {
						this.response.add(inputLine);
					}
					in.close();
					if (this.debugMode > 1) {
						debug("Response:\n" + this.getResponse());
					}
				}
			}


		} catch (IOException e) {
			return createErrorMessage(99, "IO error url: " + this.url, e);
		}

		return err;
	}

	/**
	 * Inner error message formater
	 *
	 * @param errNum - error code
	 * @param header - error header
	 * @param e  - exception or null
	 * @return error code
	 */
	private int createErrorMessage(int errNum, String header, Exception e) {
		this.err = errNum;
		this.errorMsg = "Error : " + errNum + " " + header + " " + stackTraceToString(e);
		if (this.debugMode > 0) {
			error(this.errorMsg);
		}
		return this.err;
	}

	/**
	 * Create bucket
	 *
	 * @param bucket - bucket name
	 * @return error code
	 */
	public int bucketCreate(String bucket) {
		if (this.debugMode > 0) {
			debug("\n");
			debug("bucketCreate");
			debug("bucketCreate bucket:", bucket);
			debug("bucketCreate sid:", sid);
		}

		String method = "PUT";

		this.path = '/' + bucket;

		// init connection
		err = init(method, false);
		if (err != 0) {
			return err;
		}

		try {
			con.connect();
		} catch (IOException e) {
			createErrorMessage(71, "IO error url: " + this.url, e);
			return err;
		}

		return read(SS_FIN, false);
	}

	/**
	 * Delete bucket
	 *
	 * @param bucket - bucket name
	 * @return error code
	 */
	public int bucketDelete(String bucket) {
		if (this.debugMode > 0) {
			debug("\n");
			debug("bucketDelete");
			debug("bucketDelete bucket:", bucket);
			debug("bucketDelete sid:", sid);
		}

		String method = "DELETE";

		this.path = '/' + bucket;

		// init connection
		err = init(method, false);
		if (err != 0) {
			return err;
		}

		try {
			con.connect();
		} catch (IOException e) {
			createErrorMessage(73, "IO error url: " + this.url, e);
			return err;
		}

		return read(SS_FIN, false);
	}

	/**
	 * Delete object
	 *
	 * @param bucket - bucket name
	 * @param object - object name
	 * @return error code
	 */
	public int delete(String bucket, String object) {
		if (this.debugMode > 0) {
			debug("\n");
			debug("delete");
			debug("delete bucket:", bucket);
			debug("delete object:", object);
			debug("delete sid:", sid);
		}

		String method = "DELETE";

		this.path = '/' + bucket + '/' + object;

		// init connection
		err = init(method, false);
		if (err != 0) {
			return err;
		}

		try {
			con.connect();
		} catch (IOException e) {
			createErrorMessage(81, "IO error url: " + this.url, e);
			return err;
		}

		return read(SS_FIN, false);
	}

	/**
	 * Object create
	 * @param bucket - bucket name
	 * @param object - object name
	 * @param chunkSize - chunk size
	 * @param btreeOrder - btree order
	 * @param meta  - key/value metadata map (optional)
	 * @return error code
	 */
	public int create(String bucket, String object, int chunkSize, int btreeOrder, Map<String, String> meta) {
		if (this.debugMode > 0) {
			debug("\n");
			debug("create");
			debug("create bucket:", bucket);
			debug("create object:", object);
			debug("create sid:", sid);
		}

		String method = "POST";
		int mode = SS_FIN;

		this.path = '/' + bucket + '/' + object;
		this.path += "?comp=streamsession";
		this.path += "&finalize";

		// init connection
		err = init(method, false);
		if (err != 0) {
			return err;
		}

		// add request headers
		con.setRequestProperty("x-ccow-object-oflags", (CCOW_O_CREATE | CCOW_O_REPLACE) + "");
		con.setRequestProperty("x-ccow-chunkmap-btree-order", btreeOrder + "");
		con.setRequestProperty("x-ccow-chunkmap-chunk-size", chunkSize + "");

		con.setRequestProperty("x-ccow-offset", "0");
		con.setRequestProperty("x-ccow-length", "0");
		con.setRequestProperty("Content-Length", "0");
		// Set metadata
		if (meta != null) {
			for (String key : meta.keySet()) {
				con.setRequestProperty("x-amz-meta-" + key, meta.get(key));
			}
		}

		try {
			con.connect();
		} catch (IOException e) {
			createErrorMessage(81, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}

	/**
	 * Write data to the object
	 *
	 * @param bucket - bucket name
	 * @param object - object name
	 * @param arr - data buffer
	 * @param off - offset
	 * @param more - true to continue stream operations, false to finish stream operations
	 * @return error code
	 */
	public int write(String bucket, String object, ByteBuffer arr[], long off, boolean more) {
		int mode = (more ? SS_CONT : SS_FIN);
		long totlen = 0;
		int i;
		for (i = 0; i < arr.length; i++) {
			arr[i].flip();
			totlen += arr[i].limit();
		}
		if (this.debugMode > 0) {
			debug("\n");
			debug("write");
			debug("write bucket", bucket);
			debug("write object", object);
			debug("write off", off);
			debug("write totlen", totlen);
			debug("write more", more);
			debug("write sid", sid);
		}

		String method = "POST";
		this.path = '/' + bucket + '/' + object;
		this.path += "?comp=streamsession";
		this.path += (mode == SS_FIN ? "&finalize" : "");

		// init connection
		err = init(method, totlen > 0);
		if (err != 0) {
			return err;
		}

		// add request headers
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		con.setRequestProperty("x-ccow-offset", off + "");
		con.setRequestProperty("x-ccow-length", totlen + "");
		con.setRequestProperty("Content-Length", totlen + "");

		WritableByteChannel channel = null;
		try {
			con.connect();

			// Send data request
			if (totlen > 0) {
				OutputStream wr = con.getOutputStream();
				channel = Channels.newChannel(wr);
				for (i = 0; i < arr.length; i++) {
					channel.write(arr[i]);
				}
				channel.close();
				wr.close();
			}
		} catch (IOException e) {
			createErrorMessage(82, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}

	/**
	 * Append to the object
	 * @param bucket - bucket name
	 * @param object - object name
	 * @param arr - data buffers
	 * @return error code
	 */
	public int append(String bucket, String object, ByteBuffer arr[]) {
		int mode = SS_FIN;

		long totlen = 0;
		int i;
		for (i = 0; i < arr.length; i++) {
			arr[i].flip();
			totlen += arr[i].limit();
		}

		if (this.debugMode > 0) {
			debug("\n");
			debug("append");
			debug("append bucket:", bucket);
			debug("append object:", object);
			debug("append sid:", sid);
			debug("append totlen:", totlen);
		}

		String method = "PUT";
		this.path = '/' + bucket + '/' + object;

		this.path += "?comp=appendblock";
		this.path += (mode == SS_FIN ? "&finalize" : "");

		// init connection
		err = init(method, totlen > 0);
		if (err != 0) {
			return err;
		}

		// add request headers
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		con.setRequestProperty("x-ccow-length", totlen + "");
		con.setRequestProperty("Content-Length", totlen + "");

		WritableByteChannel channel = null;
		try {
			con.connect();

			// Send data request
			if (totlen > 0) {
				OutputStream wr = con.getOutputStream();
				channel = Channels.newChannel(wr);
				for (i = 0; i < arr.length; i++) {
					channel.write(arr[i]);
				}
				channel.close();
				wr.close();
			}
		} catch (IOException e) {
			createErrorMessage(83, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}

	/**
	 * Update block inside the object
	 * @param bucket - bucket name
	 * @param object - object name
	 * @param arr - data buffers
	 * @param off - block offset
	 * @return error code
	 */
	public int writeBlock(String bucket, String object, ByteBuffer arr[], long off) {
		int mode = SS_FIN;

		long totlen = 0;
		int i;
		for (i = 0; i < arr.length; i++) {
			arr[i].flip();
			totlen += arr[i].limit();
		}

		if (this.debugMode > 0) {
			debug("\n");
			debug("writeBlock");
			debug("writeBlock bucket:", bucket);
			debug("writeBlock object:", object);
			debug("writeBlock off:", off);
			debug("writeBlock sid:", sid);
			debug("writeBlock totlen:", totlen);
		}

		String method = "PUT";
		this.path = '/' + bucket + '/' + object;
		this.path += "?comp=randwrblock";
		this.path += (mode == SS_FIN ? "&finalize" : "");

		// init connection
		err = init(method, totlen > 0);
		if (err != 0) {
			return err;
		}

		// add request headers
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		con.setRequestProperty("x-ccow-offset", off + "");
		con.setRequestProperty("x-ccow-length", totlen + "");
		con.setRequestProperty("Content-Length", totlen + "");

		WritableByteChannel channel = null;
		try {
			con.connect();

			// Send data request
			if (totlen > 0) {
				OutputStream wr = con.getOutputStream();
				channel = Channels.newChannel(wr);
				for (i = 0; i < arr.length; i++) {
					channel.write(arr[i]);
				}
				channel.close();
				wr.close();
			}
		} catch (IOException e) {
			createErrorMessage(84, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}

	/**
	 * Get object headers (stats)
	 * @param bucket - bucket name
	 * @param object - object name
	 * @return error code
	 */
	public int head(String bucket, String object) {
		if (this.debugMode > 0) {
			debug("\n");
			debug("head");
			debug("head bucket:", bucket);
			debug("head object:", object);
			debug("head sid:", sid);
		}

		String method = "HEAD";

		this.path = '/' + bucket + '/' + object + "?comp=streamsession";

		this.path += "&finalize";

		// init connection
		err = init(method, false);
		if (err != 0) {
			return err;
		}

		// add request headers
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		con.setRequestProperty("x-ccow-offset", "0");
		con.setRequestProperty("x-ccow-length", "0");

		try {
			con.connect();
		} catch (IOException e) {
			createErrorMessage(85, "Connection error url: " + this.url, e);
			return err;
		}

		return read(SS_FIN, false);
	}


	/**
	 * Get whole object as string
	 * @param bucket - bucket name
	 * @param object - object name
	 * @return error code
	 */
	public int get(String bucket, String object) {
		return get(bucket, object, false);
	}


	/**
	 * Get whole object
	 * @param bucket - bucket name
	 * @param object - object name
	 * @param binary - true get binary response, false get string response
	 * @return error code
	 */
	public int get(String bucket, String object, boolean binary) {
		int e = this.head(bucket, object);
		if (e != 0)
			return e;

		long length = this.getLogicalSize();
		return get(bucket, object, 0, length, false, binary);
	}

	/**
	 * Read block from the object
	 * @param bucket - bucket name
	 * @param object - object name
	 * @param off -offset
	 * @param len - block length
	 * @param more - true to do stream writes, false to finish stream writes
	 * @param binary - true get binary response, false get string response
	 * @return error code
	 */
	public int get(String bucket, String object, long off, long len, boolean more, boolean binary) {
		int mode = (more ? SS_CONT : SS_FIN);

		if (this.debugMode > 0) {
			debug("\n");
			debug("get");
			debug("get bucket:", bucket);
			debug("get object:", object);
			debug("get off:", off);
			debug("get len:", len);
			debug("get more:", more);
			debug("get sid:", sid);
		}

		String method = "GET";

		this.path = '/' + bucket + '/' + object + "?comp=streamsession";
		this.path += (mode == SS_FIN) ? "&finalize" : "";

		// init connection
		err = init(method, false);
		if (err != 0) {
			return err;
		}

		// add request headers
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		con.setRequestProperty("x-ccow-offset", off + "");
		con.setRequestProperty("x-ccow-length", len + "");

		try {
			con.connect();
		} catch (IOException e) {
			createErrorMessage(86, "Connection error url: " + this.url, e);
			return err;
		}

		return read(mode, binary);
	}

	/**
	 * Create key/value store
	 * @param bucket - bucket name
	 * @param object - key/value object store name
	 * @param contentType - content type
	 * @param chunkSize - chunk size
	 * @param btreeOrder - btree order
	 * @return error code
	 */
	public int keyValueCreate(String bucket, String object, String contentType,
			int chunkSize, int btreeOrder) {
		if (this.debugMode > 0) {
			debug("\n");
			debug("keyValueCreate");
			debug("keyValueCreate bucket:", bucket);
			debug("keyValueCreate object:", object);
			debug("keyValueCreate sid:", sid);
		}

		this.path = '/' + bucket + '/' + object + "?comp=kv";
		this.path += "&finalize";

		// init connection
		err = init("POST", false);
		if (err != 0) {
			return err;
		}

		// add request headers
		con.setRequestProperty("Content-Type", contentType);
		con.setRequestProperty("Content-Length", "0");

		con.setRequestProperty("x-ccow-object-oflags", (CCOW_O_CREATE | CCOW_O_REPLACE) + "");
		con.setRequestProperty("x-ccow-chunkmap-btree-order", btreeOrder + "");
		con.setRequestProperty("x-ccow-chunkmap-chunk-size", chunkSize + "");

		try {
			con.connect();
		} catch (IOException e) {
			return createErrorMessage(93, "IO error url: " + this.url, e);
		}

		return read(SS_FIN, false);
	}

	/**
	 * Insert key/value pair(s)
	 * @param bucket - bucket name
	 * @param object - key/value object name
	 * @param key - key if format equals application/octet-stream or empty
	 * @param values - key/value json if format equals application/json is used
	 * @param fmt - application/octet-stream - key is used,
	 *              application/json values json are used
	 * @param more - true to continue stream operations, false to finish stream operations
	 * @return error code
	 */
	public int keyValuePost(String bucket, String object, String key,
			String values, String fmt, boolean more) {
		String method = "POST";
		int mode = (more ? SS_CONT : SS_FIN);

		if (this.debugMode > 0) {
			debug("\n");
			debug("keyValuePost");
			debug("keyValuePost bucket:", bucket);
			debug("keyValuePost object:", object);
			debug("keyValuePost key:", key);
			debug("keyValuePost values:", values);
			debug("keyValuePost format:", fmt);
			debug("keyValuePost more:", more);
			debug("keyValuePost sid:", sid);
		}

		this.path = '/' + bucket + '/' + object + "?comp=kv";

		this.path += (mode == SS_FIN) ? "&finalize" : "";
		if (!"".equals(key))
			this.path += "&key=" + key;

		// init connection
		err = init(method, values.length() > 0);
		if (err != 0) {
			return err;
		}

		// add reuqest headers
		con.setRequestProperty("Content-Type", fmt);
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		con.setRequestProperty("Content-Length", "" + values.length());

		try {
			con.connect();
			// Send post request
			if (values.length() > 0) {
				OutputStream wr = con.getOutputStream();
				wr.write(values.getBytes(Charset.forName("UTF-8")));
				wr.flush();
				wr.close();
			}
		} catch (IOException e) {
			createErrorMessage(91, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}


	/**
	 * Insert key/value pair(s)
	 * @param bucket - bucket name
	 * @param object - key/value object name
	 * @param key - key if format equals application/octet-stream or empty
	 * @param values - key/value list format key;value
	 * @param fmt - application/octet-stream - key is used,
	 *              application/json values json are used
	 * @param more - true to continue stream operations, false to finish stream operations
	 * @return error code
	 */
	public int keyValuePost(String bucket, String object, String key,
			List<KeyValue> values, String fmt, boolean more) {
		String method = "POST";
		int mode = (more ? SS_CONT : SS_FIN);

		if (this.debugMode > 0) {
			debug("\n");
			debug("keyValuePost");
			debug("keyValuePost bucket:", bucket);
			debug("keyValuePost object:", object);
			debug("keyValuePost key:", key);
			debug("keyValuePost values:", values.size());
			debug("keyValuePost format:", fmt);
			debug("keyValuePost more:", more);
			debug("keyValuePost sid:", sid);
		}

		this.path = '/' + bucket + '/' + object + "?comp=kv";

		this.path += (mode == SS_FIN) ? "&finalize" : "";
		if (!"".equals(key))
			this.path += "&key=" + key;

		// init connection
		err = init(method, values.size() > 0);
		if (err != 0) {
			return err;
		}

		// add reuqest headers
		con.setRequestProperty("Content-Type", fmt);
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		long length = 0;
		int n = 0;
		int i;
		for (i=0; i < values.size(); i++) {
			if (n > 0)
				length += 1;
			length += values.get(i).length();
			n++;
		}

		con.setRequestProperty("Content-Length", "" + length);

		try {
			con.connect();
			// Send post request
			if (length > 0) {
				OutputStream wr = con.getOutputStream();
				n = 0;
				for (i=0; i < values.size(); i++) {
					if (n > 0) {
						wr.write(NL);
					}
				    wr.write(values.get(i).toString().getBytes(Charset.forName("UTF-8")));
				    n++;
				}
				wr.flush();
				wr.close();
			}
		} catch (IOException e) {
			createErrorMessage(91, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}



	/**
	 * Insert key/value pair(s)
	 * @param bucket - bucket name
	 * @param object - key/value object name
	 * @param key - key if format equals application/octet-stream or empty
	 * @param values - key/value list format key;value
	 * @param fmt - application/octet-stream - key is used,
	 *              application/json values json are used
	 * @param more - true to continue stream operations, false to finish stream operations
	 * @return error code
	 */
	public int keyValueStringPost(String bucket, String object, String key,
			List<String> values, String fmt, boolean more) {
		String method = "POST";
		int mode = (more ? SS_CONT : SS_FIN);

		if (this.debugMode > 0) {
			debug("\n");
			debug("keyValuePost");
			debug("keyValuePost bucket:", bucket);
			debug("keyValuePost object:", object);
			debug("keyValuePost key:", key);
			debug("keyValuePost values:", values.size());
			debug("keyValuePost format:", fmt);
			debug("keyValuePost more:", more);
			debug("keyValuePost sid:", sid);
		}

		this.path = '/' + bucket + '/' + object + "?comp=kv";

		this.path += (mode == SS_FIN) ? "&finalize" : "";
		if (!"".equals(key))
			this.path += "&key=" + key;

		// init connection
		err = init(method, values.size() > 0);
		if (err != 0) {
			return err;
		}

		// add reuqest headers
		con.setRequestProperty("Content-Type", fmt);
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		long length = 0;
		int n = 0;
		int i;
		for (i=0; i < values.size(); i++) {
			if (n > 0)
				length += 1;
			length += values.get(i).length();
			n++;
		}

		con.setRequestProperty("Content-Length", "" + length);

		try {
			con.connect();
			// Send post request
			if (length > 0) {
				OutputStream wr = con.getOutputStream();
				n = 0;
				for (i=0; i < values.size(); i++) {
					if (n > 0) {
						wr.write(NL);
					}
				    wr.write(values.get(i).getBytes(Charset.forName("UTF-8")));
				    n++;
				}
				wr.flush();
				wr.close();
			}
		} catch (IOException e) {
			createErrorMessage(91, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}


	/**
	 * Delete key/values
	 * @param bucket - bucket name
	 * @param object - key/value object name
	 * @param key - key if format equals application/octet-stream or empty
	 * @param values - key/value json if format equals application/json is used
	 * @param fmt - application/octet-stream - key is used,
	 *              application/json values json are used
	 * @param more - true to continue stream operations, false to finish stream operations
	 * @return error code
	 */
	public int keyValueDelete(String bucket, String object, String key,
			String values, String fmt, boolean more) {
		String method = "DELETE";
		int mode = (more ? SS_CONT : SS_FIN);

		if (this.debugMode > 0) {
			debug("\n");
			debug("keyValueDelete");
			debug("keyValueDelete bucket:", bucket);
			debug("keyValueDelete object:", object);
			debug("keyValueDelete key:", key);
			debug("keyValueDelete values:", values);
			debug("keyValueDelete format:", fmt);
			debug("keyValueDelete more:", more);
			debug("keyValueDelete sid:", sid);
		}

		this.path = '/' + bucket + '/' + object + "?comp=kv";
		this.path += (mode == SS_FIN) ? "&finalize" : "";
		if (!"".equals(key))
			this.path += "&key=" + key;

		// init connection
		err = init(method, values.length() > 0);
		if (err != 0) {
			return err;
		}

		// add reuqest headers
		con.setRequestProperty("Content-Type", fmt);
		if (sid != null)
			con.setRequestProperty("x-session-id", sid);

		con.setRequestProperty("Content-Length", "" + values.length());

		try {
			con.connect();
			// Send delete values
			if (values.length() > 0) {
				OutputStream wr = con.getOutputStream();
				wr.write(values.getBytes(Charset.forName("UTF-8")));
				wr.flush();
				wr.close();
			}
		} catch (IOException e) {
			createErrorMessage(92, "IO error url: " + this.url, e);
			return err;
		}

		return read(mode, false);
	}

	/**
	 * Get key/value list
	 * @param bucket - bucket name
	 * @param object - object name
	 * @param key - start key
	 * @param count - maximum output records count
	 * @param values: true return value, false don't
	 * @param fmt - output format text/csv or application/json
	 * @return error code
	 */
	public int keyValueList(String bucket, String object, String key,
			int count, boolean values, String fmt) {
		if (this.debugMode > 0) {
			debug("\n");
			debug("keyValueList");
			debug("keyValueList bucket:", bucket);
			debug("keyValueList object:", object);
			debug("keyValueList count:", count);
			debug("keyValueList key:", key);
			debug("keyValueList values:", values);
			debug("keyValueList format:", fmt);
			debug("keyValueList sid:", sid);
		}

		this.lastFormat = fmt;
		this.path = '/' + bucket + '/' + object + "?comp=kv";

		if (!"".equals(key))
			this.path += "&key=" + key;
		if (count > 0)
			this.path += "&maxresults=" + count;
		if (values)
			this.path += "&values=1";

		// init connection
		err = init("GET", false);
		if (err != 0) {
			return err;
		}

		// add reuqest headers
		con.setRequestProperty("Content-Type", fmt);
		con.setRequestProperty("Content-Length", "0");

		try {
			con.connect();
		} catch (IOException e) {
			createErrorMessage(93, "Connection error url: " + this.url, e);
			return err;
		}

		return read(SS_FIN, false);
	}

	/**
	 * Get last response as string
	 * @return - response string
	 */
	public String getResponseAsString() {
		StringWriter sr = new StringWriter();
		int n = 0;
		for (String r: this.response) {
			if (n > 0) {
				sr.write("\n");
			}
			sr.write(r);
			n++;
		}
		return sr.toString();
	}

	/**
	 * Get last response as KayValues
	 * @return - response string
	 */
	public List<KeyValue> getResponseAsKeyValue() {
		ArrayList<KeyValue> res = new ArrayList<KeyValue>(this.response.size());
		if ("text/csv".equals(this.lastFormat)) { // csv case
			for (String r: this.response) {
				int p = r.indexOf(';');
				if (p > 0) {
					String key = r.substring(0, p);
					String value = r.substring(p + 1);
					res.add(new KeyValue(key, value));
				}
			}
		}
		return res;
	}


	/**
	 * Get last response as String
	 * @return - response as string
	 */
	public ArrayList<String> getResponse() {
		return response;
	}

	/**
	 * Get last response as byte array
	 * @return - response as string
	 */
	public byte[] getByteResponse() {
		if (this.byteResponse == null)
			return null;
		return this.byteResponse.toByteArray();
	}

}
