package edu.jlime.rpc.message;

import java.io.Serializable;
import java.util.UUID;

public class Address implements Serializable {

	UUID id;

	public Address(UUID id) {
		super();
		this.id = id;
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
		if (!(obj instanceof Address))
			return false;
		return id.equals(((Address) obj).id);
	}

	@Override
	public String toString() {
		return id.toString();
	}

	private static final Address noAddr = new Address(new UUID(0, 0));

	public static Address noAddr() {
		return noAddr;
	}
}
