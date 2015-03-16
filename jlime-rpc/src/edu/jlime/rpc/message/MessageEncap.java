package edu.jlime.rpc.message;

import edu.jlime.core.transport.Address;
import edu.jlime.util.ByteBuffer;

public class MessageEncap extends Message {

	Message msg;

	public MessageEncap(Header h, Address from, Address to,
			Message msg) {
		super(h, from, to);
		this.msg = msg;
	}

	@Override
	protected byte[] build() {
		return msg.toByteArray();
	}

	@Override
	public ByteBuffer getDataBuffer() {
		return msg.getDataBuffer();
	}

	@Override
	public int getSize() {
		return getHeaderSize() + msg.getSize();
	}

	@Override
	protected ByteBuffer[] buildBuffers() {
		return msg.toByteBuffers();
	}

}
