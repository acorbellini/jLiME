package edu.jlime.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

public class ByteBuffer {

	protected ByteArrayOutputStream bos;

	private static final int INIT_SIZE = 256;

	byte[] buffered;

	public int readPos = 0;

	public int limit = 0;

	// private int init = 0;

	public ByteBuffer() {
		this(INIT_SIZE);
	}

	public ByteBuffer(byte[] buffered) {
		this(buffered, buffered.length);
	}

	public ByteBuffer(byte[] data, int length) {
		this.buffered = data;
		this.limit = length;
		// bos = new ByteArrayOutputStream();
	}

	public ByteBuffer(int i) {
		this(new byte[i], 0);
	}

	public ByteBuffer(byte[] buffered, int wrapSize, int init) {
		this(buffered, wrapSize);
		this.readPos = init;
	}

	public ByteBuffer(ByteBuffer buff, int from, int to) {
		this.readPos = from;
		this.limit = to;
		this.buffered = buff.buffered;
	}

	public int getInt() {
		int val = DataTypeUtils.byteArrayToInt(buffered, readPos);
		readPos += 4;
		return val;
	}

	public long getLong() {
		long val = DataTypeUtils.byteArrayToLong(buffered, readPos);
		readPos += 8;
		return val;

	}

	public ByteBuffer setOffset(int off) {
		this.readPos = off;
		return this;
	}

	public boolean getBoolean() {
		boolean val = (buffered[readPos] & 0xF) == 0xF;
		readPos++;
		return val;
	}

	public byte get() {
		byte val = buffered[readPos];
		readPos++;
		return val;
	}

	public float getFloat() {
		java.nio.ByteBuffer buff = java.nio.ByteBuffer.wrap(buffered, readPos,
				4);
		readPos += 4;
		return buff.getFloat();
	}

	public boolean hasRemaining() {
		return readPos < limit;
	}

	public byte[] getRawByteArray() {
		// byte[] raw = ByteArrayCache.get(writePos - readPos);
		byte[] raw = new byte[limit - readPos];
		System.arraycopy(buffered, readPos, raw, 0, limit - readPos);
		return raw;
	}

	public int size() {
		return limit - readPos;
	}

	public void ensureCapacity(int i) {
		if (limit + i > buffered.length)
			// byte[] copy = buffered;
			// byte[] bufferedExtended = ByteArrayCache
			// .get(buffered.length == 0 ? INIT_SIZE : buffered.length * 2);
			setSize((limit + i) * 2);

	}

	private void setSize(int i) {
		byte[] bufferedExtended = new byte[buffered.length == 0 ? INIT_SIZE : i];
		System.arraycopy(buffered, 0, bufferedExtended, 0, buffered.length);
		buffered = bufferedExtended;
	}

	public void clear() {
		// buffered = new byte[INIT_SIZE];
		limit = 0;
		readPos = 0;
	}

	public void putRawByteArray(byte[] data, int l) {
		putRawByteArray(data, 0, l);
	}

	public int getOffset() {
		return readPos;
	}

	public int getWritePos() {
		return limit;
	}

	public void reset() {
		limit = 0;
		readPos = 0;
	}

	public byte[] get(int length) {
		byte[] val = Arrays.copyOfRange(buffered, readPos, readPos + length);
		readPos += length;
		return val;
	}

	public byte[] build() {
		if (buffered.length != limit - readPos) {
			// byte[] ret = Arrays.copyOf(buffered, writePos);
			byte[] ret = Arrays.copyOfRange(buffered, readPos, limit);
			// ByteArrayCache.put(buffered);
			return ret;
		} else
			return buffered;
	}

	public void put(Byte val) {
		ensureCapacity(1);
		buffered[limit] = val;
		limit++;
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

	public void putDouble(double o) {
		putLong(Double.doubleToLongBits(o));
	}

	public double getDouble() {
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
		List<byte[]> ret = new ArrayList<>(2 * size);
		for (int i = 0; i < size; i++) {
			ret.add(getByteArray());
		}
		return ret;
	}

	// public List<ByteBuffer> getByteBufferList() {
	// int size = getInt();
	// List<ByteBuffer> ret = new ArrayList<>(size + size / 2);
	// for (int i = 0; i < size; i++) {
	// int wrapSize = getInt();
	// if (wrapSize > 5)
	// System.out.println("What");
	// ByteBuffer wrap = new ByteBuffer(buffered, readPos + wrapSize,
	// readPos);
	// ret.add(wrap);
	// readPos += wrapSize;
	// }
	// return ret;
	// }

	public List<Long> getLongList() {
		int size = getInt();
		List<Long> ret = new ArrayList<>(size + size / 2);
		for (int i = 0; i < size; i++) {
			ret.add(getLong());
		}
		return ret;
	}

	public void padTo(int maximumSize) {
		if (maximumSize <= limit)
			return;
		ensureCapacity(maximumSize - limit);
		limit = maximumSize;
	}

	public void putShort(short blockMagic) {
		putRawByteArray(java.nio.ByteBuffer.allocate(2).putShort(blockMagic)
				.array());
	}

	public void putRawByteArray(byte[] data) {
		putRawByteArray(data, 0, data.length);
	}

	public void putRawByteArray(byte[] data, int offset, int lenght) {
		putRawByteArray(data, offset, lenght, limit);
	}

	public void putRawByteArray(byte[] data, int offset, int lenght, int pos) {
		ensureCapacity(lenght);
		System.arraycopy(data, offset, buffered, pos, lenght);
		limit += lenght;
	}

	public void putInt(int i) {
		ensureCapacity(4);
		DataTypeUtils.intToByteArray(i, limit, buffered);
		limit += 4;
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

	public byte[] getBuffered() {
		return buffered;
	}

	public void putStringArray(String[] array) {
		putInt(array.length);
		for (String l : array) {
			putString(l);
		}
	}

	public String[] getStringArray() {
		int s = getInt();
		String[] ret = new String[s];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = getString();
		}
		return ret;
	}

	public void putLongArray(long[] array) {
		putInt(array.length);
		for (long i : array) {
			putLong(i);
		}
	}

	public long[] getLongArray() {
		int size = getInt();
		long[] ret = new long[size];
		for (int j = 0; j < ret.length; j++) {
			ret[j] = getLong();
		}
		return ret;
	}

	public String getString() {
		int stringLength = getInt();
		if (stringLength == 0)
			return "";

		byte[] bytes = get(stringLength);
		return new String(bytes);
	}

	public byte[] getByteArray() {
		int length = getInt();
		return get(length);
	}

	public UUID getUUID() {
		return new UUID(getLong(), getLong());
	}

	public byte[] getShortByteArray() {
		byte l = get();
		return get(l);
	}

	public Set<String> getSet() {
		Set<String> ret = new HashSet<>();
		int num = getInt();
		for (int i = 0; i < num; i++)
			ret.add(getString());
		return ret;
	}

	public Map<String, String> getMap() {
		int size = getInt();
		Map<String, String> ret = new HashMap<>();
		for (int i = 0; i < size; i++) {
			ret.put(getString(), getString());
		}
		return ret;
	}

	public void putRange(byte[] original, int originalOffset, int length) {
		putByteArray(Arrays.copyOfRange(original, originalOffset,
				originalOffset + length));
	}

	public void putBoolean(boolean b) {
		put((byte) (b ? 0xF : 0x0));
	}

	public void putLong(long l) {
		ensureCapacity(8);
		DataTypeUtils.longToByteArray(l, buffered, limit);
		limit += 8;

		// putRawByteArray(DataTypeUtils.longToByteArray(l));
	}

	public void putByteArray(byte[] data) {
		putInt(data.length);
		putRawByteArray(data);
	}

	public void putObject(Object o) throws IOException {
		if (bos == null)
			bos = new ByteArrayOutputStream();

		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(o);
		oos.reset();

		byte[] ba = bos.toByteArray();
		bos.reset();
		putByteArray(ba);
	}

	public void putUUID(UUID jobID) {
		putLong(jobID.getMostSignificantBits());
		putLong(jobID.getLeastSignificantBits());
	}

	public void putSet(Set<String> tags) {
		putInt(tags.size());
		for (String string : tags)
			putString(string);
	}

	public void putShortByteArray(byte[] build) {
		put((byte) build.length);
		putRawByteArray(build);
	}

	public void putFloat(float v) {
		byte[] asbytes = java.nio.ByteBuffer.allocate(4).putFloat(v).array();
		putRawByteArray(asbytes);
	}

	public void putString(String s) {
		if (s == null || s.isEmpty()) {
			putInt(0);
			return;
		}

		byte[] stringAsBytes = s.getBytes();
		putInt(stringAsBytes.length);
		putRawByteArray(stringAsBytes);
	}

	public void putMap(Map<String, String> data) {
		putInt(data.size());
		for (Entry<String, String> e : data.entrySet()) {
			putString(e.getKey());
			putString(e.getValue());
		}
	}

	public void putInt(int pos, int value) {
		DataTypeUtils.intToByteArray(value, pos, buffered);
	}

	public java.nio.ByteBuffer asByteBuffer() {
		return java.nio.ByteBuffer.wrap(buffered, readPos, limit - readPos);
	}
}
