package com.nexenta.edgex;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.nexenta.edgex.EdgexClient;
import com.nexenta.edgex.KeyValue;
import com.nexenta.edgex.util.CommonBase;
import com.nexenta.edgex.util.ConvertUtil;
import com.nexenta.edgex.util.EncodeUtil;
import com.nexenta.edgex.util.JSONUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class KeyValueTest extends CommonBase {
   static EdgexClient edgex;

   static int err = 0;
   static String bucket = "bkkvobj";
   static String object = "kvobj";
   static String notobject = "abcdkvobj";

	@BeforeClass
	public static void setUp() throws Exception {
		out("\n\nSetup edgex client");
		String url = (System.getProperty("edgex") != null ? System.getProperty("edgex") : "http://localhost:9982");
		out("URL", url);
		String key = System.getProperty("key");
		String secret = System.getProperty("secret");
		out("key", key);
		out("secret", secret);

		edgex = new EdgexClient(url,
				EdgexClient.DEFAULT_CONNECTION_TIMEOUT,
				EdgexClient.DEFAULT_READ_TIMEOUT,
				key,
				secret);
		edgex.setDebugMode(2);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		edgex.close();
		out("\n\nTear down edgex client");
	}

	@Test
	public void test00() {
		out("\n\ncan create new bucket");
		err = edgex.bucketCreate(bucket);
		assertEquals(err, 0);
	}

	@Test
	public void test10() {
		out("\n\ncan create new empty KV object");
		err = edgex.keyValueCreate(bucket, object, "application/json",
				EdgexClient.DEFAULT_CHUNKSIZE, EdgexClient.DEFAULT_BTREE_ORDER);
		out("err: ", err);
		assertEquals(err, 0);
	}

	@Test
	public void test15() {
		out("\n\ncan get key/value object header");
		err = edgex.head(bucket, object);
		assertEquals(err, 0);
	}

	@Test
	public void test17() {
		out("\n\ncan get error for notobject");
		err = edgex.head(bucket, notobject);
		assertEquals(err, 404);
	}


	@Test
	public void test20() {
		out("\n\ncan insert one as json");
		err = edgex.keyValuePost(bucket, object, "", ConvertUtil.strToJson("k1","v1"),
				"application/json", true);
		assertEquals(err, 0);
	}

	@Test
	public void test30() {
		out("\n\ncan insert two as json");
		err = edgex.keyValuePost(bucket, object, "",
				ConvertUtil.strToJson("k2","v2","k3","v3"),
				"application/json", true);
		assertEquals(err, 0);
	}

	@Test
	public void test40() {
		out("\n\ncan insert two as text/csv");
		List<String>arr = Arrays.asList("k7;v7", "k8;v8");
		err = edgex.keyValueStringPost(bucket, object, "",
				arr,"text/csv", true);
		assertEquals(err, 0);
	}

	@Test
	public void test42() {
		out("\n\ncan insert two as text/csv");
		ArrayList<KeyValue>arr = new ArrayList<KeyValue>();
		arr.add(new KeyValue("k9","v9"));
		arr.add(new KeyValue("k10","v10"));
		err = edgex.keyValuePost(bucket, object, "",
				arr,"text/csv", true);
		assertEquals(err, 0);
	}


	@Test
	public void test45() {
		out("\n\ncan finalize on empty insert");
		err = edgex.keyValuePost(bucket, object, "",
				"",
				"text/csv", false);
		assertEquals(err, 0);
	}


	@Test
	public void test50() {
		out("\n\ncan insert one as binary and finalize");
		err = edgex.keyValuePost(bucket, object, "k4", "value4",
				"application/octet-stream", false);
		assertEquals(err, 0);
	}

	@Test
	public void test60() {
		out("\n\ncan get k1");
		err = edgex.keyValueList(bucket, object, "k1", 1, true, "text/csv");
		assertEquals(err, 0);
	}

	@Test
	public void test70() {
		out("\n\ncan get k2 and k3");
		err = edgex.keyValueList(bucket, object, "k2", 2, true, "application/json");
		if (err != 0) {
			assertEquals(err, 0);
		} else {
			assertEquals(JSONUtil.sizeJSON(edgex.getResponseAsString()), 2);
		}
	}

	@Test
	public void test75() {
		out("\n\ncan list as text/csv");
		err = edgex.keyValueList(bucket, object, "", 100, true, "text/csv");
		assertEquals(err, 0);
		if (err == 0) {
			assertEquals(edgex.getResponse().size(), 8);
		}
	}

	@Test
	public void test80() {
		out("\n\ncan get k4");
		err = edgex.keyValueList(bucket, object, "k4", 1, true, "text/csv");
		assertEquals(err, 0);
		List<KeyValue>arr = edgex.getResponseAsKeyValue();
		for (KeyValue kv: arr) {
			out("KeyValue: " + kv.toString());
		}
	}

	@Test
	public void test82() {
		out("\n\ncan list json");
		err = edgex.keyValueList(bucket, object, "", 100, false, "application/json");
		assertEquals(err, 0);
		assertEquals(JSONUtil.sizeJSON(edgex.getResponseAsString()), 8);
	}


	@Test
	public void test84() {
		out("\n\ncan delete two as json");
		err = edgex.keyValueDelete(bucket, object, "",
				ConvertUtil.strToJson("k2","","k3",""),
				"application/json", false);
		assertEquals(err, 0);
	}

	@Test
	public void test86() {
		out("\n\ncan delete one");
		err = edgex.keyValueDelete(bucket, object, "k4", "",
				"application/octet-stream", false);
		assertEquals(err, 0);
	}


	@Test
	public void test88() {
		out("\n\ncan list as JSON");
		err = edgex.keyValueList(bucket, object, "", 100, true, "application/json");
		assertEquals(JSONUtil.sizeJSON(edgex.getResponseAsString()), 5);
	}


	@Test
	public void test90() {
		out("\n\ncan insert one hundred as text/csv");
		ArrayList<KeyValue>arr = new ArrayList<KeyValue>();
		for (int i=0; i<100; i++) {
			arr.add(new KeyValue("key" + i,"value" + i));
		}
		err = edgex.keyValuePost(bucket, object, "",
				arr,"text/csv", false);
		assertEquals(err, 0);
	}

	@Test
	public void test92() {
		out("\n\ncan list one hundred");
		err = edgex.keyValueList(bucket, object, "key", 100, true, "text/csv");
		assertEquals(err, 0);
	}

	@Test
	public void test94() {
		out("\n\ncan Encoded key list");
		int num = 50;
		List<KeyValue> res;

		for (int n=0; n<num; n++) {
			   edgex.keyValuePost(bucket, object, "xkey"+EncodeUtil.encodeLong(n), "value"+n,
					"application/octet-stream", false);
		}

		edgex.keyValueList(bucket, object, "xkey"+EncodeUtil.encodeLong(num - 10), 100, true, "text/csv");

		res = edgex.getResponseAsKeyValue();
		out(" Result count: " + res.size());


		edgex.keyValueList(bucket, object, "xkey"+EncodeUtil.encodeLong(num), 100, true, "text/csv");
		res = edgex.getResponseAsKeyValue();
		out(" Result count: " + res.size());
	}


	@Test
	public void test98() {
		out("\n\ncan delete kvstore");
		err = edgex.delete(bucket, object);
		assertEquals(err, 0);
	}

	@Test
	public void test99() {
		out("\n\ncan delete bucket");
		err = edgex.bucketDelete(bucket);
		assertEquals(err, 0);
	}

}

