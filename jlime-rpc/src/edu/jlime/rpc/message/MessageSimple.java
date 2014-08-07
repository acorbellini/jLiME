package edu.jlime.rpc.message;

import edu.jlime.util.ByteBuffer;

public class MessageSimple extends Message {

	ByteBuffer data;

	public MessageSimple(Header h, ByteBuffer data, Address from, Address to) {
		super(h, from, to);
		this.data = data;
	}

	@Override
	protected byte[] build() {
		return data.build();
	}

	@Override
	public ByteBuffer getDataBuffer() {
		return data;
	}
}
