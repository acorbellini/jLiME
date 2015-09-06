package edu.jlime.rpc.message;

import edu.jlime.util.ByteBuffer;

public class Header {

	public static int HEADER = 5;

	MessageType type;

	ByteBuffer headerData;

	public Header(MessageType type) {
		this.type = type;
		this.headerData = new ByteBuffer(HEADER);
		this.headerData.put(type.getId());
		this.headerData.putInt(0);
	}

	private Header() {
	}

	// public Header(MessageType type, ByteBuffer data) {
	// this.type = type;
	// this.headerData = data;
	// headerData.put(type.getId());
	// headerData.putInt(0);
	// }

	public MessageType getType() {
		return type;
	}

	public byte[] toBytes() {
		headerData.putInt(1, headerData.size() - HEADER);
		return headerData.build();
		// byte[] data = headerData.build();
		// ByteBuffer writer = new ByteBuffer(HEADER + data.length);
		// writer.put(getType().getId());
		// writer.putByteArray(data);
		// return writer.build();
	}

	public static Header fromBytes(ByteBuffer reader) {
		MessageType type = MessageType.fromID(reader.get());
		int size = reader.getInt();
		ByteBuffer headerData = new ByteBuffer(reader, reader.readPos,
				reader.readPos + size);
		Header header = new Header();
		header.type = type;
		header.headerData = headerData;
		return header;
	}

	public ByteBuffer getBuffer() {
		return headerData;
	}

	public void setType(MessageType type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "TYPE: " + type + " Header size: " + size();
	}

	public int size() {
		return headerData.size();
	}

	public ByteBuffer buildBuffer() {
		headerData.putInt(1, headerData.size() - HEADER);
		return headerData;
	}

	public void clear() {
		this.headerData.clear();
		this.headerData.put(type.getId());
		this.headerData.putInt(0);
	}
}