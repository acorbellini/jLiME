package edu.jlime.core.transport;

import java.io.Serializable;
import java.util.UUID;

public final class Address implements Comparable<Address>, Serializable {

	UUID id;

	public Address() {
		this.id = UUID.randomUUID();
	}

	public Address(UUID id) {
		super();
		this.id = id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Address other = (Address) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public int compareTo(Address o) {
		return id.compareTo(o.id);
	}

	public UUID getId() {
		return id;
	}

	private static Address noAddr = new Address(new UUID(0, 0));

	public static Address noAddr() {
		return noAddr;
	}

}
