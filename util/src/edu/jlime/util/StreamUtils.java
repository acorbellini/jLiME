package edu.jlime.util;

import java.io.IOException;
import java.io.InputStream;

public class StreamUtils {
	public static byte[] read(InputStream inputStream, int size)
			throws IOException {
		byte[] data = new byte[size];
		int read = 0;
		int total = 0;
		while (total != size
				&& (read = inputStream.read(data, total, size - total)) != -1) {
			total += read;
		}
		if (read == -1)
			throw new IOException("Stream closed before finishing read.");
		return data;
	}

	public static int readInt(InputStream is) throws IOException {
		return IntUtils.byteArrayToInt(StreamUtils.read(is, 4));
	}

	public static byte[] readFully(InputStream bis) throws IOException {
		ByteBuffer buffer = new ByteBuffer();

		boolean done = false;
		while (!done) {
			byte[] buffered = new byte[4096];
			int read = 0;
			int total = 0;
			while (total != 4096
					&& (read = bis.read(buffered, total, 4096 - total)) != -1) {
				total += read;
			}
			if (read == -1)
				done = true;

			if (total != 0)
				buffer.putRawByteArray(buffered, total);
		}
		return buffer.build();
	}
}
