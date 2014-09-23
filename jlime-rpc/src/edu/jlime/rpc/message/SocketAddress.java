package edu.jlime.rpc.message;

import java.net.InetSocketAddress;

public class SocketAddress {
	private AddressType type;

	private InetSocketAddress to;

	public SocketAddress(InetSocketAddress addr, AddressType type) {
		this.to = addr;
		this.setType(type);
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
		return "SocketAddress [type=" + type + ", to=" + to + "]";
	}

}
