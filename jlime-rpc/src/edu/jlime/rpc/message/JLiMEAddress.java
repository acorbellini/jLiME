package edu.jlime.rpc.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import edu.jlime.core.transport.Address;

public class JLiMEAddress implements Address {

	UUID id;

	public JLiMEAddress(UUID id) {
		super();
		this.id = id;
	}

	public JLiMEAddress() {
		this(UUID.randomUUID());
	}

	public UUID getId() {
		return id;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof JLiMEAddress))
			return false;
		return id.equals(((JLiMEAddress) obj).id);
	}

	@Override
	public String toString() {
		return id.toString();
	}

	private static final JLiMEAddress noAddr = new JLiMEAddress(new UUID(0, 0));

	public static JLiMEAddress noAddr() {
		return noAddr;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(id.getMostSignificantBits());
		out.writeLong(id.getLeastSignificantBits());
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.id = new UUID(in.readLong(), in.readLong());

	}

	@Override
	public int compareTo(Address o) {
		return this.id.compareTo(((JLiMEAddress) o).id);
	}
}
