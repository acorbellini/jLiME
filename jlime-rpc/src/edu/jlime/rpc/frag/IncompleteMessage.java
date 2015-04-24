package edu.jlime.rpc.frag;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.util.ByteBuffer;

public class IncompleteMessage {

	Logger log = Logger.getLogger(IncompleteMessage.class);

	byte[] buff;

	ConcurrentHashMap<Integer, Boolean> added = new ConcurrentHashMap<>();

	AtomicInteger acc;

	int id;

	AtomicBoolean completed = new AtomicBoolean(false);

	private int total;

	public IncompleteMessage(int total, int id) {
		this.buff = new byte[total];
		this.total = total;
		this.acc = new AtomicInteger(0);
		this.id = id;
	}

	public boolean addPart(int offsetInOriginal, byte[] data) {

		Boolean res = added.putIfAbsent(offsetInOriginal, true);
		if (res != null)
			return false;

		System.arraycopy(data, 0, buff, offsetInOriginal, data.length);

		int current = acc.getAndAdd(data.length);

		if (log.isDebugEnabled())
			log.debug("Received incomplete message " + id
					+ " current accumulated:" + (current + data.length)
					+ " of total:" + total + " offset: " + offsetInOriginal);
		return true;
	};

	public boolean isCompleted() {
		return acc.get() == total;
	}

	public byte[] getBuff() {
		return buff;
	}

	public boolean contains(int offset) {
		return added.containsKey(offset);
	}

	public boolean setCompleted() {
		return isCompleted() && completed.compareAndSet(false, true);
	}
}