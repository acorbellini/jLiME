package edu.jlime.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class IntUtils {

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
}
