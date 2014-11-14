package edu.jlime.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

public class XZ implements Compressor {

	@Override
	public byte[] compress(byte[] in) {
		ByteArrayOutputStream bas = new ByteArrayOutputStream(in.length);
		try {

			XZOutputStream comp = new XZOutputStream(bas, new LZMA2Options());

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
	public byte[] uncompress(byte[] in) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteArrayInputStream bas = new ByteArrayInputStream(in);
		try {
			XZInputStream comp = new XZInputStream(bas);
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
		return CompressionType.XZ;
	}

}
