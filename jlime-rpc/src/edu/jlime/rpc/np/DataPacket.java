package edu.jlime.rpc.np;

import java.net.InetSocketAddress;

import edu.jlime.util.ByteBuffer;

public class DataPacket {

	ByteBuffer reader;

	private InetSocketAddress addr;

	public DataPacket(ByteBuffer buffer, InetSocketAddress socketAddress) {
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
