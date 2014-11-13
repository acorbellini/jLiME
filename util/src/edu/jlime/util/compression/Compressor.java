package edu.jlime.util.compression;

import edu.jlime.util.compression.Compression.CompressionType;

public interface Compressor {
	public byte[] compress(byte[] in);

	public byte[] uncompress(byte[] in);

	public CompressionType getType();
}
