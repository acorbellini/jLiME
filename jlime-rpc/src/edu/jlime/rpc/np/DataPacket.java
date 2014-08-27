package edu.jlime.rpc.np;

import java.net.InetSocketAddress;

import edu.jlime.util.Buffer;

public class DataPacket {

	Buffer reader;

	private InetSocketAddress addr;

	public DataPacket(Buffer buffer,
			InetSocketAddress socketAddress) {
		this.reader = buffer;
		this.addr = socketAddress;
	}

	public InetSocketAddress getAddr() {
		return addr;
	}

	public void setAddr(InetSocketAddress addr) {
		this.addr = addr;
	}
}
