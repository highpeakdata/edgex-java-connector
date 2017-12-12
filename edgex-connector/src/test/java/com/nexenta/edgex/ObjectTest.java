package com.nexenta.edgex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Map;

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
   static String bucket = "bkobj";
   static String notbucket = "abcdefg";
   static String object = "obj";
   static String notobject = "abcdobj";
   static String stest = "Abcdefg";
   static String sappend = "oooooo";
   static String sblock = "bb";

	@BeforeClass
	public static void setUp() throws Exception {
		out("\n\nSetup edgex client");
		String url = (System.getProperty("edgex") != null ? System.getProperty("edgex") : "http://localhost:9982");
		out("URL", url);
		edgex = new EdgexClient(url,
				EdgexClient.DEFAULT_CONNECTION_TIMEOUT,
				EdgexClient.DEFAULT_READ_TIMEOUT);
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
		err = edgex.create(bucket, object, EdgexClient.DEFAULT_CHUNKSIZE, EdgexClient.DEFAULT_BTREE_ORDER, meta);
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
		assertTrue(edgex.getResponseAsString().equals(stest + stest));
	}

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


		err = edgex.append(bucket, object, ConvertUtil.strToBuffer(sappend, sappend));
		assertEquals(err, 0);

		err = edgex.head(bucket, object);
		assertEquals(err, 0);
		long len2 = edgex.getLogicalSize();


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

