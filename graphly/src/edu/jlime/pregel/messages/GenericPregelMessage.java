package edu.jlime.pregel.messages;

public class GenericPregelMessage<T> extends PregelMessage {

	T v;

	public GenericPregelMessage(String msgType, long from, long to, T val) {
		super(msgType, from, to);
		this.v = val;
	}

	public T getV() {
		return v;
	}

	@Override
	public PregelMessage getCopy() {
		GenericPregelMessage genericPregelMessage = new GenericPregelMessage(
				getType(), from, to, v);
		genericPregelMessage.setBroadcast(isBroadcast());
		return genericPregelMessage;
	}
}