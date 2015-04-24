package edu.jlime.pregel.messages;

public class GenericPregelMessage extends PregelMessage {

	Object v;

	public GenericPregelMessage(long from, long to, Object val) {
		super(from, to);
		this.v = val;
	}

	@Override
	public Object getV() {
		return v;
	}

	@Override
	public void setV(Object v) {
		this.v = v;
	}

	@Override
	public PregelMessage getCopy() {
		GenericPregelMessage genericPregelMessage = new GenericPregelMessage(
				from, to, v);
		genericPregelMessage.setBroadcast(isBroadcast());
		return genericPregelMessage;
	}

}