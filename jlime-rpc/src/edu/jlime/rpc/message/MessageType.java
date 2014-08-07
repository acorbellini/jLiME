package edu.jlime.rpc.message;

public enum MessageType {

	DISCOVERY((byte) 0),

	DISCOVERY_RESPONSE((byte) 1),

	DISCOVERY_CONFIRM((byte) 2),

	PING((byte) 3),

	PONG((byte) 4),

	DATA((byte) 5),

	RESPONSE((byte) 6),

	FRAG((byte) 7),

	FC((byte) 8),

	FC_ACK((byte) 9),

	ACK_SEQ((byte) 10),

	ACK((byte) 11),

	BUNDLE((byte) 12);

	private byte id;

	private MessageType(byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}

	public static MessageType fromID(byte id) {
		for (MessageType mt : MessageType.values()) {
			if (mt.getId() == id)
				return mt;
		}
		return null;
	}

	public static void main(String[] args) {
		for (int i = 0; i < 30; i++) {
			System.out.println(MessageType.fromID((byte) i));

		}
	}
}
