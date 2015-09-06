package edu.jlime.pregel.queues;

import edu.jlime.pregel.messages.PregelMessage;

public class FloatArrayPregelMessage extends PregelMessage {

	private float[] val;

	public FloatArrayPregelMessage(String msgType, long from, long to, float[] v) {
		super(msgType, from, to);
		this.val = v;
	}

	public float[] getVal() {
		return val;
	}

	@Override
	public PregelMessage getCopy() {
		return new FloatArrayPregelMessage(getType(), from, to, val);
	}

}
