package edu.jlime.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

public class ByteBuffer {

	private static final int INIT_SIZE = 8096;

	private byte[] buffered;

	private int readPos = 0;

	private int writePos = 0;

	private ByteArrayOutputStream bos;

	private boolean built = false;

	public ByteBuffer() {
		this(INIT_SIZE);
	}

	public ByteBuffer(byte[] buffered) {
		this(buffered, buffered.length);
	}

	public ByteBuffer(byte[] data, int length) {
		this.buffered = data;
		this.writePos = length;
		bos = new ByteArrayOutputStream();
	}

	public ByteBuffer(int i) {
		this(DEFByteArrayCache.get(i), 0);
	}

	public String getString() {
		int stringLength = getInt();
		if (stringLength == 0)
			return null;
		String s = new String(buffered, readPos, stringLength);
		readPos += stringLength;
		return s;
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

	public void setOffset(int off) {
		this.readPos = off;
	}

	public boolean getBoolean() {
		boolean val = (buffered[readPos] & 0xF) == 0xF;
		readPos++;
		return val;
	}

	public byte[] getByteArray() {
		int length = getInt();
		return get(length);
	}

	public byte get() {
		byte val = buffered[readPos];
		readPos++;
		return val;
	}

	public int getOffset() {
		return readPos;
	}

	public byte[] get(int length) {
		byte[] val = Arrays.copyOfRange(buffered, readPos, readPos + length);
		readPos += length;
		return val;
	}

	public UUID getUUID() {
		return new UUID(getLong(), getLong());
	}

	public float getFloat() {
		java.nio.ByteBuffer buff = java.nio.ByteBuffer.wrap(buffered, readPos,
				4);
		readPos += 4;
		return buff.getFloat();
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

	public boolean hasRemaining() {
		return readPos < writePos;
	}

	public byte[] getRawByteArray() {
		byte[] raw = DEFByteArrayCache.get(writePos - readPos);
		System.arraycopy(buffered, readPos, raw, 0, writePos - readPos);
		return raw;
	}

	public int size() {
		return writePos;
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

	private void ensureCapacity(int i) {
		if (built)
			System.out.println("CANNOT WRITE AFTER BUFFER IS BUILT.");
		while (writePos + i > buffered.length) {
			byte[] copy = buffered;
			byte[] bufferedExtended = DEFByteArrayCache
					.get(buffered.length == 0 ? INIT_SIZE : buffered.length * 2);
			System.arraycopy(buffered, 0, bufferedExtended, 0, buffered.length);
			buffered = bufferedExtended;
			DEFByteArrayCache.put(copy);
		}

	}

	public void putLong(long l) {
		putRawByteArray(DataTypeUtils.longToByteArray(l));
	}

	public void putByteArray(byte[] data) {
		putInt(data.length);
		putRawByteArray(data);
	}

	public void putObject(Object o) throws IOException {
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

	public void putFront(byte[] build) {
		ensureCapacity(build.length);
		System.arraycopy(buffered, 0, buffered, build.length, writePos);
		writePos += build.length;
		System.arraycopy(build, 0, buffered, 0, build.length);
	}

	private void putLongFront(long l) {
		putFront(DataTypeUtils.longToByteArray(l));
	}

	public void put(Byte val) {
		ensureCapacity(1);
		buffered[writePos] = val;
		writePos++;
	}

	public void putRawByteArray(byte[] data) {
		ensureCapacity(data.length);
		System.arraycopy(data, 0, buffered, writePos, data.length);
		writePos += data.length;
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

	public void putInt(int i) {
		putRawByteArray(DataTypeUtils.intToByteArray(i));
	}

	public byte[] build() {
		built = true;
		byte[] ret = Arrays.copyOf(buffered, writePos);
		DEFByteArrayCache.put(buffered);
		return ret;
	}

	public void putUUIDFront(UUID id) {
		putLongFront(id.getLeastSignificantBits());
		putLongFront(id.getMostSignificantBits());
	}

	public void clear() {
		buffered = new byte[INIT_SIZE];
		writePos = 0;
		readPos = 0;
	}

	@Override
	public String toString() {
		return Arrays.toString(buffered);
	}

	public void putMap(Map<String, String> data) {
		putInt(data.size());
		for (Entry<String, String> e : data.entrySet()) {
			putString(e.getKey());
			putString(e.getValue());
		}
	}
}
