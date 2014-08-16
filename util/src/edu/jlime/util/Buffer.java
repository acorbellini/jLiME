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

public abstract class Buffer {

	protected ByteArrayOutputStream bos;

	public abstract void put(Byte val);

	public abstract void putRawByteArray(byte[] data, int l);

	public abstract void putRawByteArray(byte[] data);

	public abstract int size();

	public abstract byte[] getRawByteArray();

	public abstract boolean hasRemaining();

	public abstract float getFloat();

	public abstract byte get();

	public abstract byte[] get(int length);

	public abstract boolean getBoolean();

	public abstract long getLong();

	public abstract int getInt();

	public abstract Object getObject() throws Exception;

	public Buffer() {
		super();
	}

	public String getString() {
		int stringLength = getInt();
		if (stringLength == 0)
			return null;

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

	public void putMap(Map<String, String> data) {
		putInt(data.size());
		for (Entry<String, String> e : data.entrySet()) {
			putString(e.getKey());
			putString(e.getValue());
		}
	}

}