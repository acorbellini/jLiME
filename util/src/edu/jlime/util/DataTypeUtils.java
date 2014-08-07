package edu.jlime.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class DataTypeUtils {

	public static int byteArrayToInt(byte[] b, int init) {
		return b[init + 3] & 0xFF | (b[init + 2] & 0xFF) << 8
				| (b[init + 1] & 0xFF) << 16 | (b[init + 0] & 0xFF) << 24;
	}

	public static int byteArrayToInt(byte[] b) {
		return byteArrayToInt(b, 0);
	}

	public static int[] byteArrayToIntArray(byte[] data) {
		int[] ret = new int[data.length / 4];
		for (int i = 0; i < (data.length / 4); i++) {
			byte[] d = new byte[] { data[i * 4 + 0], data[i * 4 + 1],
					data[i * 4 + 2], data[i * 4 + 3] };
			ret[i] = byteArrayToInt(d);
		}
		return ret;
	}

	public static byte[] intArrayToByteArray(int[] data) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * 4);
		IntBuffer intBuffer = byteBuffer.asIntBuffer();
		intBuffer.put(data);
		return byteBuffer.array();
	}

	public static byte[] intToByteArray(int a) {
		byte[] ret = new byte[4];
		ret[3] = (byte) (a & 0xFF);
		ret[2] = (byte) ((a >> 8) & 0xFF);
		ret[1] = (byte) ((a >> 16) & 0xFF);
		ret[0] = (byte) ((a >> 24) & 0xFF);
		return ret;
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

	public static long byteArrayToLong(byte[] bytes, int offset) {
		long l = 0;
		for (int i = offset; i < offset + 8; i++) {
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}

}
