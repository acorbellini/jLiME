package edu.jlime.collections.adjacencygraph.mappers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;

import edu.jlime.core.transport.Address;

public class LocalAddress implements Address {
	private static AtomicInteger count = new AtomicInteger(0);
	private int id;

	public LocalAddress() {
		this.id = count.incrementAndGet();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
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
		LocalAddress other = (LocalAddress) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.write(id);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.id = in.readInt();
	}

	@Override
	public int compareTo(Address o) {
		return Integer.compare(id, ((LocalAddress) o).id);
	}

}
