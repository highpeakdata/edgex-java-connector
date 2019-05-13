package com.nexenta.edgex.test;

import static org.junit.Assert.fail;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.nexenta.edgex.util.CommonBase;
import com.nexenta.edgex.util.EncodeUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EncodeTest extends CommonBase {
	static int NUMRUN = 1000000;

	@BeforeClass
	public static void setUp() throws Exception {
		out("setup");
	}

	@AfterClass
	public static void tearDown() throws Exception {
		out("tear down");
	}

	@Test
	public void test10() {
		for (int n = 0; n < NUMRUN; n++) {
			byte i1 = (byte) ThreadLocalRandom.current().nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE);
			String s1 = EncodeUtil.encodeByte(i1);
			if (EncodeUtil.decodeByte(s1) != i1) {
				error("Code error : " + i1 + " : " + s1);
				fail("Decode error 1");
			}
			byte i2 = (byte) ThreadLocalRandom.current().nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE);
			String s2 = EncodeUtil.encodeByte(i2);
			if (EncodeUtil.decodeByte(s2) != i2) {
				error("Code error : " + i2 + " : " + s2);
				fail("Decode error 2");
				return;
			}

			if (i1 < i2 && s1.compareTo(s2) >= 0) {
				error("Compare error 1");
				fail("Compare error 1");
				return;
			}
			if (i1 > i2 && s1.compareTo(s2) <= 0) {
				fail("Compare error 2");
				return;
			}
			if (i1 == i2 && s1.compareTo(s2) != 0) {
				fail("Compare error 3");
				return;
			}
		}
	}

	@Test
	public void test20() {
		for (int n = 0; n < NUMRUN; n++) {
			short i1 = (short) ThreadLocalRandom.current().nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
			String s1 = EncodeUtil.encodeShort(i1);
			if (EncodeUtil.decodeShort(s1) != i1) {
				error("Code error : " + i1 + " : " + s1);
				fail("Decode error 1");
			}
			short i2 = (short) ThreadLocalRandom.current().nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
			String s2 = EncodeUtil.encodeShort(i2);
			if (EncodeUtil.decodeShort(s2) != i2) {
				error("Code error : " + i2 + " : " + s2);
				fail("Decode error 2");
				return;
			}

			if (i1 < i2 && s1.compareTo(s2) >= 0) {
				error("Compare error 1");
				fail("Compare error 1");
				return;
			}
			if (i1 > i2 && s1.compareTo(s2) <= 0) {
				fail("Compare error 2");
				return;
			}
			if (i1 == i2 && s1.compareTo(s2) != 0) {
				fail("Compare error 3");
				return;
			}
		}
	}



	@Test
	public void test30() {
		for (int n = 0; n < NUMRUN; n++) {
			int i1 = ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);
			String s1 = EncodeUtil.encodeInt(i1);
			if (EncodeUtil.decodeInt(s1) != i1) {
				error("Code error : " + i1 + " : " + s1);
				fail("Decode error 1");
			}
			int i2 = ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);
			String s2 = EncodeUtil.encodeInt(i2);
			if (EncodeUtil.decodeInt(s2) != i2) {
				error("Code error : " + i2 + " : " + s2);
				fail("Decode error 2");
				return;
			}

			if (i1 < i2 && s1.compareTo(s2) >= 0) {
				error("Compare error 1");
				fail("Compare error 1");
				return;
			}
			if (i1 > i2 && s1.compareTo(s2) <= 0) {
				fail("Compare error 2");
				return;
			}
			if (i1 == i2 && s1.compareTo(s2) != 0) {
				fail("Compare error 3");
				return;
			}
		}
	}


	@Test
	public void test40() {
		for (int n = 0; n < NUMRUN; n++) {
			long i1 = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE/2, Long.MAX_VALUE/2);
			String s1 = EncodeUtil.encodeLong(i1);
			if (EncodeUtil.decodeLong(s1) != i1) {
				error("Code error : " + i1 + " : " + s1);
				fail("Decode error 1");
			}
			long i2 = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE/2, Long.MAX_VALUE/2);
			String s2 = EncodeUtil.encodeLong(i2);
			if (EncodeUtil.decodeLong(s2) != i2) {
				error("Code error : " + i2 + " : " + s2);
				fail("Decode error 2");
				return;
			}

			if (i1 < i2 && s1.compareTo(s2) >= 0) {
				error("Compare error 1");
				fail("Compare error 1");
				return;
			}
			if (i1 > i2 && s1.compareTo(s2) <= 0) {
				fail("Compare error 2");
				return;
			}
			if (i1 == i2 && s1.compareTo(s2) != 0) {
				fail("Compare error 3");
				return;
			}
		}
	}

}
