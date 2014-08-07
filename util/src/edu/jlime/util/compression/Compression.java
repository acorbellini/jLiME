package edu.jlime.util.compression;

import org.iq80.snappy.Snappy;

import edu.jlime.util.DataTypeUtils;

public class Compression {

	public static byte[] compress(byte[] buf) {
		return Snappy.compress(buf);
	}

	public static byte[] compress(int[] data) {
		return Snappy.compress(DataTypeUtils.intArrayToByteArray(data));
	}

	public static byte[] uncompress(byte[] data) {
		return Snappy.uncompress(data, 0, data.length);
	}

	public static int[] uncompressIntArray(byte[] data) {
		byte[] array = Snappy.uncompress(data, 0, data.length);
		return DataTypeUtils.byteArrayToIntArray(array);
	}
}
