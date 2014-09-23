package edu.jlime.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.util.Arrays;

public class ByteBuffer extends Buffer {

	private static final int INIT_SIZE = 8096;

	byte[] buffered;

	int readPos = 0;

	int writePos = 0;

	public ByteBuffer() {
		this(INIT_SIZE);
	}

	public ByteBuffer(byte[] buffered) {
		this(buffered, buffered.length);
	}

	public ByteBuffer(byte[] data, int length) {
		this.buffered = data;
		this.writePos = length;
		// bos = new ByteArrayOutputStream();
	}

	public ByteBuffer(int i) {
		this(new byte[i], 0);
	}

	@Override
	public int getInt() {
		int val = DataTypeUtils.byteArrayToInt(buffered, readPos);
		readPos += 4;
		return val;
	}

	@Override
	public long getLong() {
		long val = DataTypeUtils.byteArrayToLong(buffered, readPos);
		readPos += 8;
		return val;

	}

	public void setOffset(int off) {
		this.readPos = off;
	}

	@Override
	public boolean getBoolean() {
		boolean val = (buffered[readPos] & 0xF) == 0xF;
		readPos++;
		return val;
	}

	@Override
	public byte get() {
		byte val = buffered[readPos];
		readPos++;
		return val;
	}

	@Override
	public float getFloat() {
		java.nio.ByteBuffer buff = java.nio.ByteBuffer.wrap(buffered, readPos,
				4);
		readPos += 4;
		return buff.getFloat();
	}

	@Override
	public boolean hasRemaining() {
		return readPos < writePos;
	}

	@Override
	public byte[] getRawByteArray() {
		byte[] raw = ByteArrayCache.get(writePos - readPos);
		System.arraycopy(buffered, readPos, raw, 0, writePos - readPos);
		return raw;
	}

	@Override
	public int size() {
		return writePos;
	}

	void ensureCapacity(int i) {
		while (writePos + i > buffered.length) {
			// byte[] copy = buffered;
			byte[] bufferedExtended = ByteArrayCache
					.get(buffered.length == 0 ? INIT_SIZE : buffered.length * 2);
			System.arraycopy(buffered, 0, bufferedExtended, 0, buffered.length);
			buffered = bufferedExtended;
		}

	}

	@Override
	public void putRawByteArray(byte[] data) {
		putRawByteArray(data, data.length);
	}

	public void clear() {
		buffered = new byte[INIT_SIZE];
		writePos = 0;
		readPos = 0;
	}

	@Override
	public void putRawByteArray(byte[] data, int l) {
		ensureCapacity(l);
		System.arraycopy(data, 0, buffered, writePos, l);
		writePos += l;
	}

	public int getOffset() {
		return readPos;
	}

	public void reset() {
		writePos = 0;
		readPos = 0;
	}

	public byte[] get(int length) {
		byte[] val = Arrays.copyOfRange(buffered, readPos, readPos + length);
		readPos += length;
		return val;
	}

	public byte[] build() {
		if (buffered.length != writePos) {
			byte[] ret = Arrays.copyOf(buffered, writePos);
			ByteArrayCache.put(buffered);
			return ret;
		} else
			return buffered;
	}

	public void put(Byte val) {
		ensureCapacity(1);
		buffered[writePos] = val;
		writePos++;
	}

	public Object getObject() throws Exception {
		int size = getInt();
		ByteArrayInputStream is = new ByteArrayInputStream(buffered, readPos,
				size);

		ObjectInputStream oos = new ObjectInputStream(is);
		Object ret = oos.readObject();
		oos.close();
		is.close();

		readPos += size;

		return ret;

	}

	public void putDouble(Double o) {
		putLong(Double.doubleToLongBits(o));
	}

	public Object getDouble() {
		return Double.longBitsToDouble(getLong());
	}
}
