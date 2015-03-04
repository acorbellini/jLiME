package edu.jlime.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamUtils {
	// private static final int BUFFER_SIZE = 32 * 1024;

	private static volatile int cont = 0;
	private static volatile int promCount = 0;

	public static byte[] read(InputStream inputStream, int size)
			throws IOException {
		int read = 0;
		int remaining = size;

		byte[] buffer = new byte[size];
		while (remaining != 0
				&& (read = inputStream
						.read(buffer, size - remaining, remaining)) != -1)
			remaining -= read;
		if (read == -1 && remaining != 0)
			throw new IOException("Stream closed before finishing read.");
		return buffer;
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

	public static void putString(DataOutput out, String name)
			throws IOException {
		byte[] bytes = name.getBytes();
		putByteArray(out, bytes);

	}

	private static void putByteArray(DataOutput out, byte[] bytes)
			throws IOException {
		putInt(out, bytes.length);
		out.write(bytes);
	}

	private static void putInt(DataOutput out, int length) throws IOException {
		out.writeInt(length);
	}

	public static void putMap(DataOutput out, Map<String, String> data)
			throws IOException {
		int size = data.size();
		putInt(out, size);
		for (Entry<String, String> e : data.entrySet()) {
			putString(out, e.getKey());
			putString(out, e.getValue());
		}
	}

	public static String readString(DataInput in) throws IOException {
		int size = readInt(in);
		return new String(readByteArray(in, size));
	}

	private static byte[] readByteArray(DataInput in, int size)
			throws IOException {
		byte[] bytes = new byte[size];
		in.readFully(bytes);
		return bytes;
	}

	private static int readInt(DataInput in) throws IOException {
		return in.readInt();
	}

	public static Map<String, String> readMap(ObjectInput in)
			throws IOException {
		Map<String, String> map = new HashMap<>();
		int size = readInt(in);
		for (int i = 0; i < size; i++) {
			map.put(readString(in), readString(in));
		}
		return map;
	}
}
