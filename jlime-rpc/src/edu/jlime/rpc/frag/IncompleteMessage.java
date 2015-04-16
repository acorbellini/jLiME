package edu.jlime.rpc.frag;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.util.ByteBuffer;

public class IncompleteMessage {

	Logger log = Logger.getLogger(IncompleteMessage.class);

	private ByteBuffer buff;

	ConcurrentHashMap<Integer, Boolean> added = new ConcurrentHashMap<>();

	int remaining;

	int id;

	private String from;

	private int total;

	public IncompleteMessage(Address from, int total, int id) {
		this.buff = new ByteBuffer(total);
		this.from = from.toString();
		this.total = total;
		this.remaining = total;
		this.id = id;
	}

	public void addPart(int offsetInOriginal, byte[] data) {

		added.put(offsetInOriginal, true);

		buff.putRawByteArray(data, 0, data.length, offsetInOriginal);

		remaining -= data.length;

		if (log.isDebugEnabled())
			log.debug("Remaining of incomplete message " + id + " :"
					+ remaining + " offset: " + offsetInOriginal);

	};

	public boolean isCompleted() {
		return remaining == 0;
	}

	public ByteBuffer getBuff() {
		return buff;
	}

	public boolean contains(int offset) {
		return added.containsKey(offset);
	}
}