package edu.jlime.rpc.data;

import edu.jlime.core.transport.Address;

public class DataMessage {

	private Address from;

	private byte[] data;

	public DataMessage(byte[] data, int msgID, Address to, Address from) {
		this.setData(data);
		this.from = from;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public Address getFrom() {
		return from;
	}

	public void setFrom(Address from) {
		this.from = from;
	}
}