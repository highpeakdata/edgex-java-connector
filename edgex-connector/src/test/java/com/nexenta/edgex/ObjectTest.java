package com.nexenta.edgex.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.nexenta.edgex.EdgexClient;
import com.nexenta.edgex.util.CommonBase;
import com.nexenta.edgex.util.ConvertUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ObjectTest extends CommonBase {
   static EdgexClient edgex;

   static int err = 0;
   static String bucket = "bkobj"  + System.currentTimeMillis();
   static String notbucket = "abcdefg" + System.currentTimeMillis();
   static String object = "obj"  + System.currentTimeMillis();
   static String contentType = "application/octet-stream";
   static String notobject = "abcdobj" + System.currentTimeMillis();
   static String stest = "Abcdefg";
   static String sappend = "oooooo";
   static String sblock = "bb";

   public static String randomString(int size) {
       byte buffer[] = new byte[size];
       Random random = new Random();
       for (int i = 0; i < size; i++)
       	   buffer[i] = (byte) (random.nextInt(26) + 'a');
       return new String(buffer);
   }

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
		edgex.setDebugMode(1);
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
	public void test04() {
		out("\n\ncan get bucket header");
		err = edgex.head(bucket, "");
		assertEquals(err, 0);
	}

	@Test
	public void test06() {
		out("\n\ncan get error for non-existing bucket");
		err = edgex.head(notbucket, "");
		assertEquals(err, 404);
	}

	@Test
	public void test10() {
		out("\n\ncan create new object");
		Map<String, String> meta = ConvertUtil.strToMap("one", "1","two", "2");
		err = edgex.create(bucket, object, EdgexClient.DEFAULT_CHUNKSIZE, EdgexClient.DEFAULT_BTREE_ORDER,
				contentType, meta);
		assertEquals(err, 0);
	}

	@Test
	public void test20() {
		out("\n\ncan get object header");
		err = edgex.head(bucket, object);
		assertEquals(err, 0);
	}

	@Test
	public void test24() {
		out("\n\ncan get error for notobject");
		err = edgex.head(bucket, notobject);
		assertEquals(err, 404);
	}

	@Test
	public void test30() {
		out("\n\ncan write to object");
		ByteBuffer arr[] = ConvertUtil.strToBuffer(stest);
		err = edgex.write(bucket, object, arr, 0, true);
		assertEquals(err, 0);

		err = edgex.write(bucket, object, arr, stest.length(), false);
		assertEquals(err, 0);
	}

	@Test
	public void test40() {
		out("\n\ncan read the object as string");
		err = edgex.get(bucket, object);
		assertEquals(err, 0);
		out("Response length: " + edgex.getResponseAsString().length());
		assertTrue(edgex.getResponseAsString().equals(stest+stest));
	}


//	@Test
//	public void test41() {
//		out("\n\ncan write/read random string");
//		String ornd = "rnd4";
//		err = edgex.create(bucket, ornd, EdgexClient.DEFAULT_CHUNKSIZE, EdgexClient.DEFAULT_BTREE_ORDER,
//				contentType, null);
//		assertEquals(err, 0);
//
//		int size = 50000;
//		int step = 100;
//		StringBuffer srnd = new StringBuffer(randomString(size));
//		String res;
//
//
//		// Add at the end
//		int num = size / step;
//		for (int i=0; i<num; i++) {
//			int off = i*step;
//			ByteBuffer arr[] = ConvertUtil.strToBuffer(srnd.substring(off, off+step));
//			err = edgex.write(bucket, ornd, arr, off, (i < (num - 1) ? true: false));
//			assertEquals(err, 0);
//		}
//
//		// Read it back
//		for (int i=0; i<num; i++) {
//			int off = i*step;
//			err = edgex.get(bucket, ornd, off, step, (i < (num - 1) ? true: false), false);
//			assertEquals(err, 0);
//			res = edgex.getResponseAsString();
//			assertTrue(srnd.substring(off, off+step).equals(res));
//		}
//
//		// Read whole
//		err = edgex.get(bucket, ornd);
//		assertEquals(err, 0);
//		res = edgex.getResponseAsString();
//		out("srnd.length: " + srnd.length());
//		out("Response length: " + res.length());
//		assertTrue(srnd.toString().equals(res));
//
//		// Set random
//		Random random = new Random();
//		for (int i=0; i<100; i++) {
//			int off = random.nextInt(size);
//			int st = random.nextInt(1000)+1;
//			String s = randomString(st);
//			out(i + " off: " + off + " s.len: "+ s.length());
//			srnd.replace(off, off+s.length(), s);
//			ByteBuffer arr[] = ConvertUtil.strToBuffer(srnd.substring(off, off+s.length()));
//			err = edgex.writeBlock(bucket, ornd, arr, off);
//			assertEquals(err, 0);
//
//			err = edgex.get(bucket, ornd);
//			assertEquals(err, 0);
//			res = edgex.getResponseAsString();
//			out("srnd.length: " + srnd.length());
//			out("Response length: " + res.length());
//
//			if (!srnd.toString().equals(res)) {
//				for (int n=0; n<srnd.length() && n<res.length(); n++) {
//					if (srnd.charAt(n) != res.charAt(n)) {
//						out("diff at: " + n);
//					}
//				}
//				System.exit(1);
//			}
//		}
//
//
//		// Read whole
//		err = edgex.get(bucket, ornd);
//		assertEquals(err, 0);
//		res = edgex.getResponseAsString();
//		out("srnd.length: " + srnd.length());
//		out("Response length: " + res.length());
//		assertTrue(srnd.toString().equals(res));
//
//		err = edgex.delete(bucket, ornd);
//		if (err != 0) {
//			error(edgex.getErrorMsg());
//		}
//		assertEquals(err, 0);
//	}

	@Test
	public void test45() {
		out("\n\ncan read the object as binary");
		err = edgex.get(bucket, object, true);
		assertEquals(err, 0);
		assertTrue(new String(edgex.getByteResponse()).equals(stest + stest));
	}


	@Test
	public void test50() {
		out("\n\ncan read part of the object");
		int off = 0;
		int len = 3;
		err = edgex.get(bucket, object, off, len, true, false);
		assertEquals(err, 0);
		assertTrue(edgex.getResponseAsString().equals(stest.substring(off, off+len)));

		off += len;
		err = edgex.get(bucket, object, off, len, false, false);
		assertEquals(err, 0);
		assertTrue(edgex.getResponseAsString().equals(stest.substring(off, off+len)));
	}

	@Test
	public void test60() {
		out("\n\ncan append to the object");

		err = edgex.head(bucket, object);
		assertEquals(err, 0);
		long len1 = edgex.getLogicalSize();
		out("len1: " + len1);


		err = edgex.append(bucket, object, ConvertUtil.strToBuffer(sappend, sappend));
		assertEquals(err, 0);

		err = edgex.head(bucket, object);
		assertEquals(err, 0);
		long len2 = edgex.getLogicalSize();
		out("len2: " + len2);


		assertTrue(len2 - len1 == sappend.length()*2);
	}


	@Test
	public void test70() {
		out("\n\ncan write random block to the object");

		// Write random block
		int off = 3;
		err = edgex.writeBlock(bucket, object, ConvertUtil.strToBuffer(sblock), off);
		assertEquals(err, 0);

		// Read it back
		err = edgex.get(bucket, object, off, sblock.length(), false, false);
		assertEquals(err, 0);

		// Compare
		assertTrue(edgex.getResponseAsString().equals(sblock));
	}


	@Test
	public void test90() {
		out("\n\ncan delete object");
		err = edgex.delete(bucket, object);
		if (err != 0) {
			error(edgex.getErrorMsg());
		}
		assertEquals(err, 0);
	}

	@Test
	public void test99() {
		out("\n\ncan delete bucket");
		err = edgex.bucketDelete(bucket);
		if (err != 0) {
			error(edgex.getErrorMsg());
		}
		assertEquals(err, 0);
	}
}

