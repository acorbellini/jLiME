package edu.jlime.rpc.message;

public enum AddressType {

	TCP((byte) 0),

	UDP((byte) 1),

	MCAST((byte) 2),

	ANY((byte) 3),

	MINA((byte) 4);

	private byte id;

	private AddressType(byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}

	public static AddressType fromID(byte id) {
		for (AddressType at : AddressType.values()) {
			if (at.getId() == id)
				return at;
		}
		return null;
	}
}
