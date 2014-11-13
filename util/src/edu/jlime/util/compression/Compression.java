package edu.jlime.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.iq80.snappy.Snappy;

import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;

public class Compression {
	static LZ4Compressor comp = LZ4Factory.nativeInstance().highCompressor();
	static LZ4FastDecompressor decomp = LZ4Factory.unsafeInstance()
			.fastDecompressor();

	public static byte[] compress(byte[] buf) {
		// return lz4compress(buf);
		// return Snappy.compress(buf);
		// return bzipcompress(buf);
		// return gzipcompress(buf);
		return xzcompress(buf);
	}

	public static byte[] uncompress(byte[] data) {
		// return lz4decompress(data);
		// return Snappy.uncompress(data, 0, data.length);
		// return bzipdecompress(data);
		// return gzipdecompress(data);
		return xzdecompress(data);
	}

	public static byte[] compress(int[] data) {
		return Snappy.compress(DataTypeUtils.intArrayToByteArray(data));
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

	public static byte[] xzcompress(byte[] in) {
		ByteArrayOutputStream bas = new ByteArrayOutputStream(in.length);
		try {

			XZCompressorOutputStream comp = new XZCompressorOutputStream(bas, 5);

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

	public static byte[] bzipcompress(byte[] in) {
		ByteArrayOutputStream bas = new ByteArrayOutputStream(in.length);
		try {

			BZip2CompressorOutputStream comp = new BZip2CompressorOutputStream(
					bas);

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

	public static byte[] gzipcompress(byte[] in) {
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

	public static byte[] xzdecompress(byte[] in) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteArrayInputStream bas = new ByteArrayInputStream(in);
		try {
			XZCompressorInputStream comp = new XZCompressorInputStream(bas);
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

	public static byte[] bzipdecompress(byte[] in) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteArrayInputStream bas = new ByteArrayInputStream(in);
		try {
			BZip2CompressorInputStream comp = new BZip2CompressorInputStream(
					bas);
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

	public static byte[] gzipdecompress(byte[] in) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
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
}
