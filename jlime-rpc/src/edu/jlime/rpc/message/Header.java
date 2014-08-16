package edu.jlime.rpc.message;

import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;

public class Header {

	MessageType type;

	ByteBuffer headerData;

	public Header(MessageType type) {
		this(type, new ByteBuffer());
	}

	public Header(MessageType type, ByteBuffer data) {
		this.type = type;
		this.headerData = data;
	}

	public MessageType getType() {
		return type;
	}

	public byte[] toBytes() {

		byte[] data = headerData.build();
		ByteBuffer writer = new ByteBuffer(1 + 1 + data.length);
		writer.put(getType().getId());
		writer.putShortByteArray(data);
		return writer.build();
	}

	public static Header fromBytes(Buffer reader) {
		MessageType type = MessageType.fromID(reader.get());
		ByteBuffer headerData = new ByteBuffer(reader.getShortByteArray());
		return new Header(type, headerData);
	}

	public ByteBuffer getBuffer() {
		return headerData;
	}

	public void setType(MessageType type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "TYPE: " + type + " Header size: " + headerData.size();
	}

	public int size() {
		return 2 + headerData.size();
	}
}