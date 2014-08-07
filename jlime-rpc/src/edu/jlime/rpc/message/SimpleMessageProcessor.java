package edu.jlime.rpc.message;

public abstract class SimpleMessageProcessor extends MessageProcessor {

	public SimpleMessageProcessor(MessageProcessor next, String name) {
		super(name);
		super.registerProcessor("next", next);
	}

	protected MessageProcessor getNext() {
		return getProc("next");
	}

	protected void sendNext(Message msg) throws Exception {
		sendToProc("next", msg);
	}
}
