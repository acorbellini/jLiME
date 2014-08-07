package edu.jlime.rpc.frag;

import java.util.HashSet;

import org.apache.log4j.Logger;

public class IncompleteMessage {

	Logger log = Logger.getLogger(IncompleteMessage.class);

	private byte[] buff;

	HashSet<Integer> added = new HashSet<>();

	int remaining;

	public IncompleteMessage(int total) {
		buff = new byte[total];
		remaining = total;
	}

	public void addPart(int offsetInOriginal, byte[] data) {

		System.arraycopy(data, 0, buff, offsetInOriginal, data.length);
		synchronized (this) {
			remaining -= data.length;
		}

		if (log.isDebugEnabled())
			log.debug("Remaining of incomplete message :" + remaining);

	};

	public boolean isCompleted() {
		return remaining == 0;
	}

	public byte[] getBuff() {
		return buff;
	}

	public boolean contains(int offset) {
		return added.contains(offset);
	}
}