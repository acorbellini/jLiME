package edu.jlime.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

public class DataTypeUtils {

	public static int byteArrayToInt(byte[] b) {
		return byteArrayToInt(b, 0);
	}

	public static byte[] intToByteArray(int a) {
		byte[] ret = new byte[4];
		intToByteArray(a, 0, ret);
		return ret;
	}

	public static int byteArrayToInt(byte[] b, int i) {
		return b[i + 3] & 0xFF | (b[i + 2] & 0xFF) << 8
				| (b[i + 1] & 0xFF) << 16 | (b[i] & 0xFF) << 24;
	}

	public static void intToByteArray(int a, int i, byte[] ret) {
		ret[i + 3] = (byte) (a & 0xFF);
		ret[i + 2] = (byte) ((a >> 8) & 0xFF);
		ret[i + 1] = (byte) ((a >> 16) & 0xFF);
		ret[i + 0] = (byte) ((a >> 24) & 0xFF);

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

	public static long byteArrayToLong(byte[] bytes, int offset) {
		long l = 0;
		for (int i = offset; i < offset + 8; i++) {
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
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
}
