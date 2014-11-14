package edu.jlime.util.compression;


public interface Compressor {
	public byte[] compress(byte[] in);

	public byte[] uncompress(byte[] in);

	public CompressionType getType();
}
