package com.nexenta.edgex.util;

public class EncodeUtil extends CommonBase {

	public static String encodeByte(byte b) {
		int i = b;
		i -= Byte.MIN_VALUE;
		return String.format("%02x", i);
	}

	public static byte decodeByte(String s) {
		int i = Integer.parseInt(s, 16);
		return (byte) (i + Byte.MIN_VALUE);
	}

	public static String encodeShort(short v) {
		int i = v;
		i -= Short.MIN_VALUE;
		return String.format("%04x", i);
	}

	public static short decodeShort(String s) {
		int i = Integer.parseInt(s, 16);
		return (short) (i + Short.MIN_VALUE);
	}

	public static String encodeInt(int v) {
		long i = v;
		i -= Integer.MIN_VALUE;
		return String.format("%08x", i);
	}

	public static int decodeInt(String s) {
		long i = Long.parseLong(s, 16);
		return (int) (i + Integer.MIN_VALUE);
	}


	public static String encodeLong(long v) {
		long i = v;
		i -= Long.MIN_VALUE;
		return String.format("%016x", i);
	}

	public static long decodeLong(String s) {
		long i = Long.parseUnsignedLong(s, 16);
		return (long) (i + Long.MIN_VALUE);
	}

}
