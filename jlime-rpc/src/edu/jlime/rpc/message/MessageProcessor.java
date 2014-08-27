package edu.jlime.rpc.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.util.RingQueue;

public abstract class MessageProcessor implements StackElement {

	private ConcurrentHashMap<MessageType, List<MessageQueue>> listeners = new ConcurrentHashMap<>();

	private Logger log = Logger.getLogger(MessageProcessor.class);

	private HashMap<String, MessageProcessor> processors = new HashMap<>();

	private RingQueue out = new RingQueue();

	private List<MessageQueue> secondaryMessage = Collections
			.synchronizedList(new ArrayList<MessageQueue>());

	private List<MessageQueue> all = Collections
			.synchronizedList(new ArrayList<MessageQueue>());

	protected boolean stopped = false;

	private String name;

	public MessageProcessor(String name) {
		this.name = name;
		Thread t = new Thread("Outcoming Timer for " + name) {
			public void run() {
				while (!stopped)
					try {
						final Object[] m = out.take();
						if (stopped)
							return;
						for (Object e : m) {
							send((Message) e);
						}

					} catch (Exception e1) {
						e1.printStackTrace();
					}
			};
		};
		t.start();

	}

	public void registerProcessor(String procName, MessageProcessor proc) {
		processors.put(procName, proc);
	}

	public MessageProcessor getProc(String name) {
		return processors.get(name);
	}

	public void start() throws Exception {
	};

	public final synchronized void stop() throws Exception {
		if (stopped)
			return;
		stopped = true;
		out.put(new MessageSimple(null, null, null, null));

		for (MessageQueue mq : secondaryMessage) {
			mq.stop();
		}

		for (MessageQueue mq : all) {
			mq.stop();
		}

		for (List<MessageQueue> list : listeners.values()) {
			for (MessageQueue mq : list) {
				mq.stop();
			}
		}

		onStop();
	};

	public void onStop() throws Exception {
	};

	protected abstract void send(Message msg) throws Exception;

	protected void notifyRcvd(Message message) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Notifying message " + message.getType() + " of size "
					+ message.getSize());
		for (MessageQueue l : all)
			l.notify(message);

		boolean notified = false;
		List<MessageQueue> list = listeners.get(message.getType());
		if (list != null) {
			for (MessageQueue l : new ArrayList<>(list)) {
				l.notify(message);
				notified = true;
			}
		}
		if (!notified) {
			for (MessageQueue any : secondaryMessage) {
				any.notify(message);
				notified = true;
			}
		}

	}

	public void sendToProc(String procName, Message msg) throws Exception {
		MessageProcessor proc = processors.get(procName);
		if (proc != null)
			proc.queue(msg);
	}

	public void queue(Message msg) {
		out.put(msg);
	}

	public synchronized void addMessageListener(MessageType type,
			MessageListener packList) {
		List<MessageQueue> list = listeners.get(type);
		if (list == null) {
			list = new ArrayList<MessageQueue>();
			listeners.put(type, list);
		}
		list.add(new MessageQueue(packList, this, type.toString()));
	}

	public void addSecondaryMessageListener(MessageListener listener) {
		secondaryMessage.add(new MessageQueue(listener, this,
				"Secondary Messages"));
	}

	public void addAllMessageListener(MessageListener listener) {
		all.add(new MessageQueue(listener, this, "All Messages"));

	}
	
	@Override
	public void cleanupOnFailedPeer(Address peer) {

	}

	public Collection<MessageProcessor> getProcessors() {
		return processors.values();
	}

	public MessageProcessor removedProc(String k) {
		return processors.remove(k);

	}

	public boolean isStopped() {
		return stopped;
	}

	public String getName() {
		return name;
	}

}