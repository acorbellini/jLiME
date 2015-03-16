package edu.jlime.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

public class DataTypeUtils {

	private static final int INT_MASK = 0xFF;
	private static final int LONG_MASK = 0xFFFF;

	public static int byteArrayToInt(byte[] b) {
		return byteArrayToInt(b, 0);
	}

	public static byte[] intToByteArray(int a) {
		byte[] ret = new byte[4];
		intToByteArray(a, 0, ret);
		return ret;
	}

	public static int byteArrayToInt(byte[] b, int i) {
		return b[i + 3] & INT_MASK | (b[i + 2] & INT_MASK) << 8
				| (b[i + 1] & INT_MASK) << 16 | (b[i] & INT_MASK) << 24;
	}

	public static void intToByteArray(int a, int i, byte[] ret) {
		ret[i + 3] = (byte) (a & INT_MASK);
		ret[i + 2] = (byte) ((a >> 8) & INT_MASK);
		ret[i + 1] = (byte) ((a >> 16) & INT_MASK);
		ret[i + 0] = (byte) ((a >> 24) & INT_MASK);

	}

	public static int[] byteArrayToIntArray(byte[] data) {
		int[] ret = new int[data.length / 4];
		for (int i = 0; i < (data.length / 4); i++)
			ret[i] = byteArrayToInt(data, i * 4);
		return ret;
	}

	public static byte[] intArrayToByteArray(int[] data) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * 4);
		IntBuffer intBuffer = byteBuffer.asIntBuffer();
		intBuffer.put(data);
		return byteBuffer.array();
	}

	public static byte[] longToByteArray(long val) {
		byte[] b = new byte[8];
		for (int i = 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static long byteArrayToLong(byte[] bytes) {
		return byteArrayToLong(bytes, 0);
	}

	public static long byteArrayToLong(byte[] b, int i) {
		return ((((long) b[0 + i] & INT_MASK) << 56)
				| (((long) b[1 + i] & INT_MASK) << 48)
				| (((long) b[2 + i] & INT_MASK) << 40)
				| (((long) b[3 + i] & INT_MASK) << 32)
				| (((long) b[4 + i] & INT_MASK) << 24)
				| (((long) b[5 + i] & INT_MASK) << 16)
				| (((long) b[6 + i] & INT_MASK) << 8) | (((long) b[7 + i] & INT_MASK)));
		// long l = 0;
		// for (int i = offset; i < offset + 8; i++) {
		// l <<= 8;
		// l ^= bytes[i] & INT_MASK;
		// }
		// return l;
	}

	public static byte[] longArrayToByteArray(long[] data) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * 8);
		LongBuffer intBuffer = byteBuffer.asLongBuffer();
		intBuffer.put(data);
		return byteBuffer.array();
	}

	public static long[] byteArrayToLongArray(byte[] data) {
		long[] ret = new long[data.length / 8];
		for (int i = 0; i < (data.length / 8); i++)
			ret[i] = byteArrayToLong(data, i * 8);
		return ret;
	}

	public static void main(String[] args) {
		byte[] b = longToByteArray(3l);
		System.out.println(byteArrayToLong(b));
	}

	public static void longToByteArray(long l, byte[] b, int pos) {
		for (int i = 7; i > 0; i--) {
			b[i + pos] = (byte) l;
			l >>>= 8;
		}
		b[0 + pos] = (byte) l;
	}
}
