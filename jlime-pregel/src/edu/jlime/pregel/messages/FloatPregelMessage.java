package edu.jlime.pregel.messages;

public class FloatPregelMessage extends PregelMessage {
	float v;

	public FloatPregelMessage(long from, long to, float val) {
		super(from, to);
		this.v = val;
	}

	@Override
	public void setV(Object v) {
		this.v = (Float) v;

	}

	@Override
	public Object getV() {
		return v;
	}

	public void setFloat(float v) {
		this.v = v;
	}

	public float getFloat() {
		return v;

	}

	@Override
	public PregelMessage getCopy() {
		FloatPregelMessage floatPregelMessage = new FloatPregelMessage(from,
				to, v);
		floatPregelMessage.setBroadcast(isBroadcast());
		return floatPregelMessage;
	}

}
