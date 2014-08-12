package edu.jlime.util;

import java.io.IOException;
import java.io.InputStream;

public class StreamUtils {
	private static final int BUFFER_SIZE = 128 * 1024;

	public static byte[] read(InputStream inputStream, int size)
			throws IOException {
		byte[] data = new byte[BUFFER_SIZE];
		int read = 0;
		int remaining = size;
		// byte[] buffer = new byte[BUFFER_SIZE];
		ByteBuffer buff = new ByteBuffer(size);
		while (remaining != 0
				&& (read = inputStream.read(data, 0,
						Math.min(remaining, BUFFER_SIZE))) != -1) {
			buff.putRawByteArray(data, read);
			remaining -= read;
		}
		if (read == -1 && remaining != size)
			throw new IOException("Stream closed before finishing read.");
		return buff.build();
	}

	public static int readInt(InputStream is) throws IOException {
		return DataTypeUtils.byteArrayToInt(StreamUtils.read(is, 4));
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
