package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.mergers.MessageMerger;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.queues.MessageQueueFactory;
import edu.jlime.pregel.queues.PregelMessageQueue;
import edu.jlime.util.Pair;
import gnu.trove.set.hash.TLongHashSet;

public class InputQueue {

	private PregelConfig config;
	private HashMap<String, PregelMessageQueue> currentQueue = new HashMap<>();
	private HashMap<String, PregelMessageQueue> readOnlyQueue = new HashMap<>();

	private HashMap<String, PregelMessageQueue> currentBroadcast = new HashMap<>();
	private HashMap<String, PregelMessageQueue> readOnlyBroadcast = new HashMap<>();

	private HashMap<Pair<String, String>, PregelMessageQueue> currentSG = new HashMap<>();
	private HashMap<Pair<String, String>, PregelMessageQueue> readOnlySG = new HashMap<>();

	public InputQueue(PregelConfig config) {
		this.config = config;
	}

	public PregelMessageQueue getQueue(String msg) {
		PregelMessageQueue ret = currentQueue.get(msg);
		if (ret == null) {
			synchronized (currentQueue) {
				ret = currentQueue.get(msg);
				if (ret == null) {
					ret = getQueueFactory(msg).getMQ();
					currentQueue.put(msg, ret);
				}
			}
		}
		return ret;
	}

	MessageQueueFactory getQueueFactory(String msgType) {
		MessageMerger merger = config.getMerger(msgType);
		MessageQueueFactory fact = merger != null ? merger.getFactory() : MessageQueueFactory.simple(null);
		return fact;
	}

	public void switchQueue() {
		{
			HashMap<String, PregelMessageQueue> aux = currentQueue;
			currentQueue = readOnlyQueue;
			readOnlyQueue = aux;
			currentQueue.clear();
		}

		{
			HashMap<String, PregelMessageQueue> aux = currentBroadcast;
			currentBroadcast = readOnlyBroadcast;
			readOnlyBroadcast = aux;
			currentBroadcast.clear();
		}

		{
			HashMap<Pair<String, String>, PregelMessageQueue> aux = currentSG;
			currentSG = readOnlySG;
			readOnlySG = aux;
			currentSG.clear();
		}
	}

	public TLongHashSet getKeys() {
		TLongHashSet ret = new TLongHashSet();
		for (Entry<String, PregelMessageQueue> e : readOnlyQueue.entrySet()) {
			ret.addAll(e.getValue().keys());
		}
		return ret;
	}

	public int broadcastSize() {
		int ret = 0;
		for (PregelMessageQueue s : readOnlyBroadcast.values())
			ret += s.size();
		return ret;

	}

	public Iterator<PregelMessage> getMessages(long currentVertex) {
		List<Iterator<PregelMessage>> ret = new ArrayList<>();
		for (Entry<String, PregelMessageQueue> e : readOnlyQueue.entrySet()) {
			PregelMessageQueue q = e.getValue();
			Iterator<PregelMessage> messages = q.getMessages(e.getKey(), currentVertex);
			if (messages != null)
				ret.add(messages);
		}

		for (Entry<String, PregelMessageQueue> e : readOnlyBroadcast.entrySet()) {
			PregelMessageQueue q = e.getValue();
			Iterator<PregelMessage> messages = q.getMessages(e.getKey(), -1l);
			if (messages != null)
				ret.add(messages);
		}

		for (Entry<Pair<String, String>, PregelMessageQueue> e : readOnlySG.entrySet()) {
			PregelMessageQueue q = e.getValue();
			if (config.getSubgraph(e.getKey().right).contains(currentVertex)) {
				Iterator<PregelMessage> messages = q.getMessages(e.getKey().left, -1l);
				if (messages != null)
					ret.add(messages);
			}
		}
		return new ConcatIterator(ret);
	}

	public PregelMessageQueue getBroadcastQueue(String msg) {
		PregelMessageQueue ret = currentBroadcast.get(msg);
		if (ret == null) {
			synchronized (currentBroadcast) {
				ret = currentBroadcast.get(msg);
				if (ret == null) {
					ret = getQueueFactory(msg).getMQ();
					currentBroadcast.put(msg, ret);
				}
			}
		}
		return ret;
	}

	public PregelMessageQueue getBroadcastSubgraphQueue(String msgType, String subGraph2) {
		Pair<String, String> p = new Pair<>(msgType, subGraph2);
		PregelMessageQueue ret = currentSG.get(p);
		if (ret == null) {
			synchronized (currentSG) {
				ret = currentSG.get(p);
				if (ret == null) {
					ret = getQueueFactory(p.left).getMQ();
					currentSG.put(p, ret);
				}
			}
		}
		return ret;
	}

	public int subgraphSize() {
		int ret = 0;
		for (PregelMessageQueue s : readOnlySG.values())
			ret += s.size();
		return ret;
	}
}
