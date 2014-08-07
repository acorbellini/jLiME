package edu.jlime.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.rpc.message.Address;

public class PeerJlime extends Peer {

	private static final long serialVersionUID = -7035925423714613683L;

	Address addr;

	public PeerJlime(Address addr, String name) {
		super(addr.getId().toString(), name);
		this.addr = addr;
	}

	public Address getAddr() {
		return addr;
	}

	@Override
	public String toString() {
		return "Name: " + getName() + " DEF Address: " + addr;
	}

	public void setAddr(Address from) {
		addr = from;
	}

	public static PeerJlime newPeer(UUID id, String name) {
		return new PeerJlime(new Address(id), name);
	}

	public UUID getId() {
		return getAddr().getId();
	}

}
