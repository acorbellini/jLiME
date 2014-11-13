package edu.jlime.util.compression;

import org.iq80.snappy.Snappy;

import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.Compression.CompressionType;

public class SnappyComp implements Compressor {

	@Override
	public byte[] compress(byte[] in) {
		return Snappy.compress(in);
	}

	@Override
	public byte[] uncompress(byte[] in) {
		return Snappy.uncompress(in, 0, in.length);
	}

	public static byte[] compress(int[] data) {
		return Snappy.compress(DataTypeUtils.intArrayToByteArray(data));
	}

	public static int[] uncompressIntArray(byte[] data) {
		byte[] array = Snappy.uncompress(data, 0, data.length);
		return DataTypeUtils.byteArrayToIntArray(array);
	}

	@Override
	public CompressionType getType() {
		return CompressionType.SNAPPY;
	}

}
