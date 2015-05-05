package edu.jlime.pregel.messages;


public class DoublePregelMessage extends PregelMessage {

	double d = 0d;

	public DoublePregelMessage(long from, long to, double val) {
		super(from, to);
		this.d = val;
	}

	@Override
	public void setV(Object v) {
		setDouble((Double) v);
	}

	private void setDouble(Double v) {
		this.d = v;
	}

	@Override
	public Object getV() {
		return d;
	}

	@Override
	public PregelMessage getCopy() {
		return new DoublePregelMessage(from, to, d);
	}

	public double getDouble() {
		return d;
	}

}
