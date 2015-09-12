package edu.jlime.pregel.messages;

public class FloatMessage extends PregelMessage {
	float v;

	public FloatMessage(String msgType, long from, long to, float val) {
		super(msgType, from, to);
		this.v = val;
	}

	public void setFloat(float v) {
		this.v = v;
	}

	public float value() {
		return v;

	}

	@Override
	public PregelMessage getCopy() {
		FloatMessage floatPregelMessage = new FloatMessage(getType(), from, to, v);
		floatPregelMessage.setBroadcast(isBroadcast());
		return floatPregelMessage;
	}

}
