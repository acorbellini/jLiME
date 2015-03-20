package edu.jlime.rpc.message;

public abstract class SimpleMessageProcessor extends MessageProcessor {

	MessageProcessor next;

	public SimpleMessageProcessor(MessageProcessor next, String name) {
		super(name);
		this.next = next;
	}

	protected MessageProcessor getNext() {
		return next;
	}

	public void sendNext(Message msg) throws Exception {
		next.send(msg);
	}
}
