package edu.jlime.util.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.iq80.snappy.Snappy;

import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;

public class Compression {
	static LZ4Compressor comp = LZ4Factory.nativeInstance().highCompressor();
	static LZ4FastDecompressor decomp = LZ4Factory.unsafeInstance()
			.fastDecompressor();

	public static byte[] compress(byte[] buf) {
		// return lz4compress(buf);
		return Snappy.compress(buf);
	}

	public static byte[] compress(int[] data) {
		return Snappy.compress(DataTypeUtils.intArrayToByteArray(data));
	}

	public static byte[] uncompress(byte[] data) {
		// return lz4decompress(data);
		return Snappy.uncompress(data, 0, data.length);
	}

	public static int[] uncompressIntArray(byte[] data) {
		byte[] array = Snappy.uncompress(data, 0, data.length);
		return DataTypeUtils.byteArrayToIntArray(array);
	}

	public static byte[] lz4compress(byte[] in) {
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(in.length);
		buff.putRawByteArray(comp.compress(in));
		return buff.build();
	}

	public static byte[] lz4decompress(byte[] in) {
		ByteBuffer buff = new ByteBuffer(in);
		int decomSize = buff.getInt();
		byte[] comp = buff.getRawByteArray();
		return decomp.decompress(comp, decomSize);

	}
}
