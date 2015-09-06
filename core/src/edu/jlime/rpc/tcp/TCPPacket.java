package edu.jlime.rpc.tcp;

public class TCPPacket {

	byte[] d;
	TCPPacketConnection c;

	public TCPPacket(byte[] d, TCPPacketConnection c) {
		this.d = d;
		this.c = c;
	}

}
