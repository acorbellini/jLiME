package edu.jlime.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import edu.jlime.core.transport.Address;

public class JGroupsAddress implements Address {

	private org.jgroups.Address address;

	public JGroupsAddress() {
	}

	public JGroupsAddress(org.jgroups.Address origin) {
		this.address = origin;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
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
		JGroupsAddress other = (JGroupsAddress) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		return true;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(address);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.address = (org.jgroups.Address) in.readObject();
	}

	@Override
	public int compareTo(Address o) {
		return this.address.compareTo(((JGroupsAddress) o).address);
	}

	public org.jgroups.Address getAddress() {
		return address;
	}

}
