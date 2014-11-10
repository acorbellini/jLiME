package edu.jlime.util;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ByteBuffer extends Buffer {

	private static final int INIT_SIZE = 32;

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
		// this(ByteArrayCache.get(i), 0);
	}

	public ByteBuffer(byte[] buffered, int wrapSize, int readPos) {
		this(buffered, wrapSize);
		this.readPos = readPos;
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

	public ByteBuffer setOffset(int off) {
		this.readPos = off;
		return this;
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
		// byte[] raw = ByteArrayCache.get(writePos - readPos);
		byte[] raw = new byte[writePos - readPos];
		System.arraycopy(buffered, readPos, raw, 0, writePos - readPos);
		return raw;
	}

	@Override
	public int size() {
		return writePos - readPos;
	}

	void ensureCapacity(int i) {
		while (writePos + i > buffered.length) {
			// byte[] copy = buffered;
			byte[] bufferedExtended = ByteArrayCache
					.get(buffered.length == 0 ? INIT_SIZE : buffered.length * 2);
			// byte[] bufferedExtended = new byte[buffered.length == 0 ?
			// INIT_SIZE
			// : buffered.length * 2];
			System.arraycopy(buffered, 0, bufferedExtended, 0, buffered.length);
			buffered = bufferedExtended;
		}

	}

	public void clear() {
		// buffered = new byte[INIT_SIZE];
		writePos = 0;
		readPos = 0;
	}

	@Override
	public void putRawByteArray(byte[] data, int l) {
		putRawByteArray(data, 0, l);
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
		if (buffered.length != writePos - readPos) {
			// byte[] ret = Arrays.copyOf(buffered, writePos);
			byte[] ret = Arrays.copyOfRange(buffered, readPos, writePos);
			// ByteArrayCache.put(buffered);
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

	public void putStringByteArrayMap(Map<String, byte[]> data) {
		putInt(data.size());
		for (Entry<String, byte[]> e : data.entrySet()) {
			putString(e.getKey());
			putByteArray(e.getValue());
		}
	}

	public Map<String, byte[]> getStringByteArrayMap() {
		int size = getInt();
		Map<String, byte[]> ret = new HashMap<>(size + size / 2);
		for (int i = 0; i < size; i++) {
			ret.put(getString(), getByteArray());
		}
		return ret;
	}

	public List<String> getStringList() {
		int size = getInt();
		List<String> list = new ArrayList<>(size + size / 2);
		for (int i = 0; i < size; i++) {
			list.add(getString());
		}
		return list;
	}

	public void putStringList(List<String> list) {
		putInt(list.size());
		for (String l : list) {
			putString(l);
		}
	}

	public void putByteArrayList(List<byte[]> keys) {
		putInt(keys.size());
		for (byte[] bs : keys) {
			putByteArray(bs);
		}
	}

	public void putByteBufferList(List<ByteBuffer> keys) {
		putInt(keys.size());
		for (ByteBuffer bs : keys) {
			putByteArray(bs.build());
		}
	}

	public void putLongList(List<Long> values) {
		putInt(values.size());
		for (Long long1 : values) {
			putLong(long1);
		}
	}

	public List<byte[]> getByteArrayList() {
		int size = getInt();
		List<byte[]> ret = new ArrayList<>(size + size / 2);
		for (int i = 0; i < size; i++) {
			ret.add(getByteArray());
		}
		return ret;
	}

	public List<ByteBuffer> getByteBufferList() {
		int size = getInt();
		List<ByteBuffer> ret = new ArrayList<>(size + size / 2);
		for (int i = 0; i < size; i++) {
			int wrapSize = getInt();
			if (wrapSize > 5)
				System.out.println("What");
			ByteBuffer wrap = new ByteBuffer(buffered, readPos + wrapSize,
					readPos);
			ret.add(wrap);
			readPos += wrapSize;
		}
		return ret;
	}

	public List<Long> getLongList() {
		int size = getInt();
		List<Long> ret = new ArrayList<>(size + size / 2);
		for (int i = 0; i < size; i++) {
			ret.add(getLong());
		}
		return ret;
	}

	public void padTo(int maximumSize) {
		if (maximumSize < writePos)
			return;
		ensureCapacity(maximumSize);
		writePos = maximumSize;
	}

	public void putShort(short blockMagic) {
		putRawByteArray(java.nio.ByteBuffer.allocate(2).putShort(blockMagic)
				.array());
	}

	public void putRawByteArray(byte[] data) {
		putRawByteArray(data, 0, data.length);
	}

	public void putRawByteArray(byte[] data, int offset, int lenght) {
		ensureCapacity(lenght);
		System.arraycopy(data, offset, buffered, writePos, lenght);
		writePos += lenght;
	}

	@Override
	public void putInt(int i) {
		ensureCapacity(4);
		DataTypeUtils.intToByteArray(i, writePos, buffered);
		writePos += 4;
	}

	public long getLong(int offset) {
		return DataTypeUtils.byteArrayToLong(buffered, readPos + offset);
	}

	public int getInteger(int offset) {
		return DataTypeUtils.byteArrayToInt(buffered, readPos + offset);
	}

	public void putIntArray(int[] array) {
		putInt(array.length);
		for (int i : array) {
			putInt(i);
		}
	}

	public int[] getIntArray() {
		int size = getInt();
		int[] ret = new int[size];
		for (int j = 0; j < ret.length; j++) {
			ret[j] = getInt();
		}
		return ret;
	}
}
