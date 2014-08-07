package edu.jlime.rpc.message;

import java.net.InetSocketAddress;
import java.util.UUID;

public class SocketAddress extends Address {

	private AddressType type;

	private InetSocketAddress to;

	public SocketAddress(UUID id, InetSocketAddress addr, AddressType type) {
		super(id);
		this.to = addr;
		this.setType(type);
	}

	public SocketAddress(Address da, InetSocketAddress addr, AddressType type) {
		this(da.getId(), addr, type);
	}

	public InetSocketAddress getSockTo() {
		return to;
	}

	public AddressType getType() {
		return type;
	}

	public void setType(AddressType type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return super.toString() + " Socket Address " + to + " Address Type "
				+ type;
	}

}
