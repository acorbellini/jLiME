package edu.jlime.pregel.messages;

public class FloatPregelMessage extends PregelMessage {
	float v;

	public FloatPregelMessage(String msgType, long from, long to, float val) {
		super(msgType, from, to);
		this.v = val;
	}

	public void setFloat(float v) {
		this.v = v;
	}

	public float getFloat() {
		return v;

	}

	@Override
	public PregelMessage getCopy() {
		FloatPregelMessage floatPregelMessage = new FloatPregelMessage(getType(), from, to, v);
		floatPregelMessage.setBroadcast(isBroadcast());
		return floatPregelMessage;
	}

}
