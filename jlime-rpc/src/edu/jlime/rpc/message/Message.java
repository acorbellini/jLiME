package edu.jlime.rpc.message;

import java.util.HashSet;

import edu.jlime.rpc.Option;
import edu.jlime.util.ByteBuffer;

public abstract class Message {

	private Header header;

	private JLiMEAddress from;

	private JLiMEAddress to;

	private HashSet<Option> sendOpts = new HashSet<Option>();

	public Message setOption(Option opt) {
		sendOpts.add(opt);
		return this;
	}

	public boolean hasOption(Option opt) {
		return sendOpts.contains(opt);
	}

	public Message(Header h, JLiMEAddress from, JLiMEAddress to) {
		this.from = from;
		this.to = to;
		header = h;
	}

	public JLiMEAddress getFrom() {
		return from;
	}

	public JLiMEAddress getTo() {
		return to;
	}

	public void setFrom(JLiMEAddress from) {
		this.from = from;
	}

	public void setTo(JLiMEAddress to) {
		this.to = to;
	}

	public MessageType getType() {
		return header.type;
	}

	protected abstract byte[] build();

	// public static DEFMessage fromBytes(DEFByteBufferReader buff,
	// DEFAddress from, DEFAddress to) {
	// MessageType type = MessageType.fromID(buff.get());
	// byte[] header = new byte[] {};
	// byte[] data = new byte[] {};
	// if (buff.hasRemaining())
	// header = buff.getShortByteArray();
	// if (buff.hasRemaining())
	// data = buff.getRawByteArray();
	// return new DEFMessage(type, data, header, from, to);
	// }

	public static Message encapsulate(Message msg, MessageType type,
			JLiMEAddress from, JLiMEAddress to) {
		return new MessageEncap(new Header(type), from, to, msg);
	};

	public Header getHeader() {
		return header;
	}

	public static MessageSimple deEncapsulate(byte[] simple, JLiMEAddress from,
			JLiMEAddress to) {
		ByteBuffer reader = new ByteBuffer(simple);
		Header h = Header.fromBytes(reader);
		ByteBuffer d = new ByteBuffer(reader.getRawByteArray());
		return new MessageSimple(h, d, from, to);
	};

	public static Message newOutDataMessage(byte[] data, MessageType type,
			JLiMEAddress to) {
		return newFullDataMessage(data, type, null, to);
	}

	public static Message newFullDataMessage(byte[] data, MessageType type,
			JLiMEAddress from, JLiMEAddress to) {
		return new MessageSimple(new Header(type), new ByteBuffer(data), from,
				to);
	};

	public byte[] toByteArray() {
		byte[] h = header.toBytes();
		byte[] data = build();
		byte[] ret = new byte[h.length + data.length];
		int c = 0;
		for (int i = 0; i < h.length; i++) {
			ret[c++] = h[i];
		}
		for (int i = 0; i < data.length; i++) {
			ret[c++] = data[i];
		}
		return ret;
	}

	public abstract ByteBuffer getDataBuffer();

	public ByteBuffer getHeaderBuffer() {
		return getHeader().getBuffer();
	}

	public byte[] getDataAsBytes() {
		return getDataBuffer().build();
	}

	public int getDataSize() {
		return getDataBuffer().size();
	}

	public static Message encapsulateOut(Message msg, MessageType type,
			JLiMEAddress to) {
		return encapsulate(msg, type, null, to);
	}

	public static Message newEmptyOutDataMessage(MessageType type,
			JLiMEAddress to) {
		return newOutDataMessage(new byte[] {}, type, to);
	}

	public static Message newEmptyBroadcastOutDataMessage(MessageType type) {
		return newEmptyOutDataMessage(type, null);
	}

	public int getSize() {
		return getHeaderSize() + getDataSize();
	}

	public int getHeaderSize() {
		return getHeader().size();
	}

	@Override
	public String toString() {
		return header + "," + to + "," + from + "," + getSize() + "b";
	}
}
