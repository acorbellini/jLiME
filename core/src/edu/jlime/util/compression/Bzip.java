package edu.jlime.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

public class Bzip implements Compressor {

	@Override
	public byte[] compress(byte[] in) {
		ByteArrayOutputStream bas = new ByteArrayOutputStream(in.length);
		try {

			BZip2CompressorOutputStream comp = new BZip2CompressorOutputStream(bas,
					BZip2CompressorOutputStream.chooseBlockSize(in.length));
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
		try {
			BZip2CompressorInputStream comp = new BZip2CompressorInputStream(bas);
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
		return CompressionType.BZIP;
	}
}
