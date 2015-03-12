package edu.jlime.rpc.frag;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;

public class IncompleteMessage {

	Logger log = Logger.getLogger(IncompleteMessage.class);

	private byte[] buff;

	ConcurrentHashMap<Integer, Boolean> added = new ConcurrentHashMap<>();

	int remaining;

	UUID id;

	private String from;

	private int total;

	public IncompleteMessage(Address from, int total, UUID id) {
		this.buff = new byte[total];
		this.from = from.toString();
		this.total = total;
		this.remaining = total;
		this.id = id;
	}

	public void addPart(int offsetInOriginal, byte[] data) {

		added.put(offsetInOriginal, true);

		System.arraycopy(data, 0, buff, offsetInOriginal, data.length);

		remaining -= data.length;

		if (log.isDebugEnabled())
			// if (remaining < 0)
			log.debug("Remaining of incomplete message " + id + " :"
					+ remaining + " offset: " + offsetInOriginal);

	};

	public boolean isCompleted() {
		return remaining == 0;
	}

	public byte[] getBuff() {
		return buff;
	}

	public boolean contains(int offset) {
		return added.containsKey(offset);
	}
}