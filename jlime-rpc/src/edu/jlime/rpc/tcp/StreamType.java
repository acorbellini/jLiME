package edu.jlime.rpc.tcp;

public enum StreamType {

	PACKET((byte) 0),

	STREAM((byte) 1);

	private byte id;

	private StreamType(byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}

	public static StreamType fromID(byte id) {
		for (StreamType st : StreamType.values())
			if (st.getId() == id)
				return st;
		return null;

	}
}