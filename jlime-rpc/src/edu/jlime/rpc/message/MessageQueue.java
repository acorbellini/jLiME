package edu.jlime.rpc.message;

import org.apache.log4j.Logger;

import edu.jlime.util.RingQueue;

public class MessageQueue {

	private Logger log = Logger.getLogger(MessageQueue.class);

	private RingQueue in = new RingQueue();

	private MessageProcessor proc;

	private MessageListener list;

	protected boolean stopped = false;

	public MessageQueue(MessageListener l, MessageProcessor p, String queueName) {
		this.proc = p;
		this.list = l;
		Thread t = new Thread("Incoming Queue Listener for " + queueName
				+ " on processor " + proc) {
			public void run() {
				while (!stopped) {
					try {
						Object[] input = in.get();
						if (stopped)
							return;
						for (Object e : input) {
							list.rcv((Message) e, proc);
						}

					} catch (Exception e1) {
						log.error("Error receiving message.", e1);
					}
				}
			};
		};
		t.start();

	}

	public void notify(Message defMessage) {
		in.add(defMessage);
	}

	public void stop() {
		stopped = true;
		in.add(Message.newEmptyBroadcastOutDataMessage(MessageType.DATA));
	}
}
