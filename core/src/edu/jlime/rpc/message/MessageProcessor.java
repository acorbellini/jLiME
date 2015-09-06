package edu.jlime.rpc.message;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;

public abstract class MessageProcessor implements StackElement {

	private ConcurrentHashMap<MessageType, List<MessageListener>> listeners = new ConcurrentHashMap<>();

	private Logger log = Logger.getLogger(MessageProcessor.class);

	private HashMap<String, MessageProcessor> processors = new HashMap<>();

	private List<MessageListener> secondaryMessage = new CopyOnWriteArrayList<MessageListener>();

	private List<MessageListener> all = new CopyOnWriteArrayList<MessageListener>();

	protected boolean stopped = false;

	private String name;

	public MessageProcessor(String name) {
		this.name = name;
	}

	public void registerProcessor(String procName, MessageProcessor proc) {
		processors.put(procName, proc);
	}

	public MessageProcessor getProc(String name) {
		return processors.get(name);
	}

	public final void start() throws Exception {
		onStart();
	};

	public void onStart() throws Exception {
	};

	public final synchronized void stop() throws Exception {
		if (stopped)
			return;
		stopped = true;

		listeners.clear();
		processors.clear();
		secondaryMessage.clear();
		all.clear();

		onStop();
	};

	protected void onStop() throws Exception {
	};

	public abstract void send(Message msg) throws Exception;

	protected void notifyRcvd(Message message) throws Exception {

		for (MessageListener l : all)
			l.rcv(message, this);

		boolean notified = false;
		MessageType type = message.getType();
		List<MessageListener> list = listeners.get(type);
		if (list != null) {
			for (MessageListener l : list) {
				l.rcv(message, this);
				notified = true;
			}
		}

		if (!notified) {
			for (MessageListener any : secondaryMessage) {
				any.rcv(message, this);
				notified = true;
			}
		}

	}

	public void sendToProc(String procName, Message msg) throws Exception {
		MessageProcessor proc = processors.get(procName);
		if (proc != null) {
			if (log.isDebugEnabled())
				log.debug("Queuing message " + msg + " on processor " + proc);
			proc.send(msg);
		}
	}

	// public void send(Message msg) throws Exception {
	// // out.put(msg);
	// send(msg);
	// }

	public synchronized void addMessageListener(MessageType type, MessageListener packList) {
		List<MessageListener> list = listeners.get(type);
		if (list == null) {
			list = new CopyOnWriteArrayList<MessageListener>();
			listeners.put(type, list);
		}
		list.add(packList);
	}

	public void addSecondaryMessageListener(MessageListener listener) {
		secondaryMessage.add(listener);
	}

	public void addAllMessageListener(MessageListener listener) {
		all.add(listener);
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