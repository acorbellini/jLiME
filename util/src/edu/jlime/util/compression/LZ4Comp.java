package edu.jlime.util.compression;

import edu.jlime.util.ByteBuffer;
import edu.jlime.util.compression.Compression.CompressionType;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4Comp implements Compressor {
	static LZ4Compressor comp = LZ4Factory.nativeInstance().highCompressor();
	static LZ4FastDecompressor decomp = LZ4Factory.unsafeInstance()
			.fastDecompressor();

	@Override
	public byte[] compress(byte[] in) {
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(in.length);
		buff.putRawByteArray(comp.compress(in));
		return buff.build();
	}

	@Override
	public byte[] uncompress(byte[] in) {
		ByteBuffer buff = new ByteBuffer(in);
		int decomSize = buff.getInt();
		byte[] comp = buff.getRawByteArray();
		return decomp.decompress(comp, decomSize);
	}

	@Override
	public CompressionType getType() {
		return CompressionType.LZ4;
	}
}
