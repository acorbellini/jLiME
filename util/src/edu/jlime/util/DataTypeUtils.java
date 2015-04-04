package edu.jlime.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.UnsignedBytes;

public class DataTypeUtils {

	private static final int INT_MASK = 0xFF;
	private static final long LONG_MASK = 0xFFl;
	private static final long INV_MASK = 0x80l;

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

	public static byte[] longToByteArray(long value) {
		byte[] ret = new byte[8];
		longToByteArray(value, ret, 0);
		return ret;
	}

	public static long byteArrayToLong(byte[] bytes) {
		return byteArrayToLong(bytes, 0);
	}

	public static long byteArrayToLong(byte[] b, int i) {
		return ((((long) b[0 + i] & LONG_MASK) << 56)
				| (((long) b[1 + i] & LONG_MASK) << 48)
				| (((long) b[2 + i] & LONG_MASK) << 40)
				| (((long) b[3 + i] & LONG_MASK) << 32)
				| (((long) b[4 + i] & LONG_MASK) << 24)
				| (((long) b[5 + i] & LONG_MASK) << 16)
				| (((long) b[6 + i] & LONG_MASK) << 8) | (((long) b[7 + i] & LONG_MASK)));
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
		{
			long[] longs = new long[] { 0, 10, 12, 3, 6, 1, -1, -5, -6,
					Long.MAX_VALUE, Long.MIN_VALUE, 12312312, 551, 12312341 };
			List<byte[]> list = new ArrayList<byte[]>();
			for (long l : longs)
				list.add(longToByteArrayOrdered(l));

			Collections.sort(list, UnsignedBytes.lexicographicalComparator());

			for (byte[] bs : list) {
				System.out.println(byteArrayToLongOrdered(bs));
			}

			// byte[] b = longToByteArrayOrdered(Long.MAX_VALUE);
			// System.out.println(byteArrayToLongOrdered(b));
			// byte[] b2 = longToByteArrayOrdered(0);
			// System.out.println(byteArrayToLongOrdered(b2));
			//
			// byte[] b4 = longToByteArrayOrdered(-1);
			// System.out.println(byteArrayToLongOrdered(b4));
			//
			// byte[] b3 = longToByteArrayOrdered(Long.MIN_VALUE);
			// System.out.println(byteArrayToLongOrdered(b3));
			//
			// System.out.println(UnsignedBytes.lexicographicalComparator()
			// .compare(b, b2));
			// System.out.println(UnsignedBytes.lexicographicalComparator()
			// .compare(b, b4));
			// System.out.println(UnsignedBytes.lexicographicalComparator()
			// .compare(b2, b3));
			// System.out.println(UnsignedBytes.lexicographicalComparator()
			// .compare(b4, b3));
			//
			// System.out.println(UnsignedBytes.lexicographicalComparator()
			// .compare(b2, b));
			// System.out.println(UnsignedBytes.lexicographicalComparator()
			// .compare(b3, b2));

		}

		// {
		// byte[] b = longToByteArray(3l);
		// System.out.println(byteArrayToLong(b));
		// byte[] b2 = longToByteArray(-1);
		// System.out.println(byteArrayToLong(b2));
		// byte[] b3 = longToByteArray(Long.MIN_VALUE);
		// System.out.println(byteArrayToLong(b3));
		// }
	}

	public static void longToByteArray(long value, byte[] b, int pos) {
		b[pos + 7] = (byte) value;
		b[pos + 6] = (byte) (value >> 8);
		b[pos + 5] = (byte) (value >> 16);
		b[pos + 4] = (byte) (value >> 24);
		b[pos + 3] = (byte) (value >> 32);
		b[pos + 2] = (byte) (value >> 40);
		b[pos + 1] = (byte) (value >> 48);
		b[pos + 0] = (byte) (value >> 56);
	}

	public static byte[] longToByteArrayOrdered(long value) {
		return new byte[] { (byte) ((value >> 56) ^ INV_MASK),
				(byte) ((value >> 48)), (byte) ((value >> 40)),
				(byte) ((value >> 32)), (byte) ((value >> 24)),
				(byte) ((value >> 16)), (byte) ((value >> 8)), (byte) (value) };
	}

	public static long byteArrayToLongOrdered(byte[] bytes) {
		return byteArrayToLongOrdered(bytes, 0);
	}

	public static long byteArrayToLongOrdered(byte[] b, int i) {
		return ((((long) (b[0 + i] ^ INV_MASK) & LONG_MASK) << 56)
				| (((long) (b[1 + i]) & LONG_MASK) << 48)
				| (((long) (b[2 + i]) & LONG_MASK) << 40)
				| (((long) (b[3 + i]) & LONG_MASK) << 32)
				| (((long) (b[4 + i]) & LONG_MASK) << 24)
				| (((long) (b[5 + i]) & LONG_MASK) << 16)
				| (((long) (b[6 + i]) & LONG_MASK) << 8) | (((long) (b[7 + i]) & LONG_MASK)));
		// long l = 0;
		// for (int i = offset; i < offset + 8; i++) {
		// l <<= 8;
		// l ^= bytes[i] & INT_MASK;
		// }
		// return l;
	}
}
