package edu.jlime.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Gzip implements Compressor {

	@Override
	public byte[] compress(byte[] in) {
		ByteArrayOutputStream bas = new ByteArrayOutputStream(in.length);
		GZIPOutputStream comp;
		try {
			comp = new GZIPOutputStream(bas);
			comp.write(in);
			comp.close();
			bas.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		byte[] byteData = bas.toByteArray();
		// System.out.println("Compression rate "
		// + (100 - ((byteData.length * 100) / in.length)));
		return byteData;
	}

	@Override
	public byte[] uncompress(byte[] in, int size) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(size);
		ByteArrayInputStream bas = new ByteArrayInputStream(in);
		GZIPInputStream comp;
		try {
			comp = new GZIPInputStream(bas);

			// BZip2CompressorInputStream comp = new
			// BZip2CompressorInputStream(bas);
			int len;
			byte[] buffer = new byte[1024];

			while ((len = comp.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}

			comp.close();
			bas.close();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] byteData = out.toByteArray();
		return byteData;
	}

	@Override
	public CompressionType getType() {
		return CompressionType.GZIP;
	}

}
