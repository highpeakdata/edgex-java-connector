package com.nexenta.edgex;

import static org.junit.Assert.fail;

import java.net.URL;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.nexenta.edgex.KeyValue;
import com.nexenta.edgex.S3Signature;
import com.nexenta.edgex.util.CommonBase;
import com.nexenta.edgex.util.ConvertUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SignatureTest extends CommonBase {
	static String secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
	static S3Signature s3signature;


	@BeforeClass
	public static void setUp() throws Exception {
		out("setup");
		s3signature = new S3Signature(2);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		out("tear down");
	}

	@Test
	public void test10() {
 		try {
			 String sig = s3signature.signRequest(secret, "GET",
						"",
						"Tue, 27 Mar 2007 19:36:42 +0000",
						"",
						new URL("http://localhost/johnsmith/photos/puppy.jpg"), null);
			out("Signature10:", sig);
			assertEquals(sig, "bWq2s1WEIj+Ydj0vQ697zp+IXMU=");
 		} catch (Exception e) {
			error("format", e);
			fail("Decode error 10");
		}
	}

	@Test
	public void test20() {
 		try {
			 String sig = s3signature.signRequest(secret, "PUT",
						"image/jpeg",
						"Tue, 27 Mar 2007 21:15:45 +0000",
						"",
						new URL("http://localhost/johnsmith/photos/puppy.jpg"), null);
			out("Signature20:", sig);
			assertEquals(sig, "MyyxeRY7whkBe+bq8fHCL/2kKUg=");
 		} catch (Exception e) {
			error("format", e);
			fail("Decode error 20");
		}
	}


	@Test
	public void test30() {
 		try {
			 String sig = s3signature.signRequest(secret, "GET",
						"",
						"Tue, 27 Mar 2007 19:42:41 +0000",
						"",
						new URL("http://localhost/johnsmith/?prefix=photos&max-keys=50&marker=puppy"), null);
			out("Signature30:", sig);
			assertEquals(sig, "htDYFYduRNen8P9ZfE/s9SuKy0U=");
 		} catch (Exception e) {
			error("format", e);
			fail("Decode error 30");
		}
	}


	@Test
	public void test40() {
 		try {
			 String sig = s3signature.signRequest(secret, "GET",
						"",
						"Tue, 27 Mar 2007 19:44:46 +0000",
						"",
						new URL("http://localhost/johnsmith/?acl"), null);
			out("Signature40:", sig);
			assertEquals(sig, "c2WLPFtWHVgbEmeEG93a4cG37dM=");
 		} catch (Exception e) {
			error("format", e);
			fail("Decode error 40");
		}
	}


	@Test
	public void test50() {
 		try {
			String sig = s3signature.signRequest(secret, "DELETE",
						"",
						"Tue, 27 Mar 2007 21:20:27 +0000",
						"",
						new URL("http://localhost/johnsmith/photos/puppy.jpg"),
						ConvertUtil.strToListMap("x-amz-date","Tue, 27 Mar 2007 21:20:26 +0000"));
			out("Signature50:", sig);
			assertEquals(sig, "lx3byBScXR6KzyMaifNkardMwNk=");
 		} catch (Exception e) {
			error("format", e);
			fail("Decode error 50");
		}
	}


	@Test
	public void test60() {
 		try {
			String sig = s3signature.signRequest(secret, "PUT",
						"application/x-download",
						"Tue, 27 Mar 2007 21:06:08 +0000",
						"4gJE4saaMU4BqNR0kLY+lw==",
						new URL("http://localhost/static.johnsmith.net/db-backup.dat.gz"),
						ConvertUtil.strToListMap("x-amz-acl","public-read",
								"X-Amz-Meta-ReviewedBy","joe@johnsmith.net,jane@johnsmith.net",
								"X-Amz-Meta-FileChecksum","0x02661779",
								"X-Amz-Meta-ChecksumAlgorithm","crc32",
								"Content-Encoding","gzip"));
			out("Signature60:", sig);
			assertEquals(sig, "ilyl83RwaSoYIEdixDQcA4OnAnc=");
 		} catch (Exception e) {
			error("format", e);
			fail("Decode error 60");
		}
	}

}
