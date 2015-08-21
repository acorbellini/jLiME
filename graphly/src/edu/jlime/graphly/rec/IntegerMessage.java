package edu.jlime.graphly.rec;

import edu.jlime.pregel.messages.PregelMessage;

public class IntegerMessage extends PregelMessage {

	public IntegerMessage(String msgType, long from, long to) {
		super(msgType, from, to);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public PregelMessage getCopy() {
		return null;
	}

}
