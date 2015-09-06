package edu.jlime.pregel.messages;

public class DoublePregelMessage extends PregelMessage {

	double d = 0d;

	public DoublePregelMessage(String msgType, long from, long to, double val) {
		super(msgType, from, to);
		this.d = val;
	}

	private void setDouble(Double v) {
		this.d = v;
	}

	@Override
	public PregelMessage getCopy() {
		return new DoublePregelMessage(getType(), from, to, d);
	}

	public double getDouble() {
		return d;
	}

}
