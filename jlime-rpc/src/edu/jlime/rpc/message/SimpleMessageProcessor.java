package edu.jlime.rpc.message;

public abstract class SimpleMessageProcessor extends MessageProcessor {

	MessageProcessor next;

	public SimpleMessageProcessor(MessageProcessor next, String name) {
		super(name);
		this.next = next;
		// super.registerProcessor("next", next);
	}

	protected MessageProcessor getNext() {
		// return getProc("next");
		return next;
	}

	protected void sendNext(Message msg) throws Exception {
		// sendToProc("next", msg);
		next.queue(msg);
	}
}
