package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.queues.FloatMessageQueue;
import edu.jlime.util.Pair;
import gnu.trove.set.hash.TLongHashSet;

public class InputQueue {

	private PregelConfig config;
	private HashMap<String, FloatMessageQueue> currentQueue = new HashMap<>();
	private HashMap<String, FloatMessageQueue> readOnlyQueue = new HashMap<>();

	private HashMap<String, FloatMessageQueue> currentBroadcast = new HashMap<>();
	private HashMap<String, FloatMessageQueue> readOnlyBroadcast = new HashMap<>();

	private HashMap<Pair<String, String>, FloatMessageQueue> currentSG = new HashMap<>();
	private HashMap<Pair<String, String>, FloatMessageQueue> readOnlySG = new HashMap<>();

	public InputQueue(PregelConfig config) {
		this.config = config;
	}

	public FloatMessageQueue getQueue(String msg) {
		FloatMessageQueue ret = currentQueue.get(msg);
		if (ret == null) {
			synchronized (currentQueue) {
				ret = currentQueue.get(msg);
				if (ret == null) {
					ret = new FloatMessageQueue(config.getMerger(msg));
					currentQueue.put(msg, ret);
				}
			}
		}
		return ret;
	}

	public void switchQueue() {
		{
			HashMap<String, FloatMessageQueue> aux = currentQueue;
			currentQueue = readOnlyQueue;
			readOnlyQueue = aux;
			currentQueue.clear();
		}

		{
			HashMap<String, FloatMessageQueue> aux = currentBroadcast;
			currentBroadcast = readOnlyBroadcast;
			readOnlyBroadcast = aux;
			currentBroadcast.clear();
		}

		{
			HashMap<Pair<String, String>, FloatMessageQueue> aux = currentSG;
			currentSG = readOnlySG;
			readOnlySG = aux;
			currentSG.clear();
		}
	}

	public TLongHashSet getKeys() {
		TLongHashSet ret = new TLongHashSet();
		for (Entry<String, FloatMessageQueue> e : readOnlyQueue.entrySet()) {
			ret.addAll(e.getValue().keys());
		}
		return ret;
	}

	public int broadcastSize() {
		int ret = 0;
		for (FloatMessageQueue s : readOnlyBroadcast.values())
			ret += s.size();
		return ret;

	}

	public Iterator<PregelMessage> getMessages(long currentVertex) {
		List<Iterator<PregelMessage>> ret = new ArrayList<>();
		for (Entry<String, FloatMessageQueue> e : readOnlyQueue.entrySet()) {
			FloatMessageQueue q = e.getValue();
			Iterator<PregelMessage> messages = q.getMessages(e.getKey(),
					currentVertex);
			if (messages != null)
				ret.add(messages);
		}

		for (Entry<String, FloatMessageQueue> e : readOnlyBroadcast
				.entrySet()) {
			FloatMessageQueue q = e.getValue();
			Iterator<PregelMessage> messages = q.getMessages(e.getKey(), -1l);
			if (messages != null)
				ret.add(messages);
		}

		for (Entry<Pair<String, String>, FloatMessageQueue> e : readOnlySG
				.entrySet()) {
			FloatMessageQueue q = e.getValue();
			if (config.getSubgraph(e.getKey().right).contains(currentVertex)) {
				Iterator<PregelMessage> messages = q
						.getMessages(e.getKey().left, -1l);
				if (messages != null)
					ret.add(messages);
			}
		}
		return new ConcatIterator(ret);
	}

	public FloatMessageQueue getBroadcastQueue(String msg) {
		FloatMessageQueue ret = currentBroadcast.get(msg);
		if (ret == null) {
			synchronized (currentBroadcast) {
				ret = currentBroadcast.get(msg);
				if (ret == null) {
					ret = new FloatMessageQueue(config.getMerger(msg));
					currentBroadcast.put(msg, ret);
				}
			}
		}
		return ret;
	}

	public FloatMessageQueue getBroadcastSubgraphQueue(String msgType,
			String subGraph2) {
		Pair<String, String> p = new Pair<>(msgType, subGraph2);
		FloatMessageQueue ret = currentSG.get(p);
		if (ret == null) {
			synchronized (currentSG) {
				ret = currentSG.get(p);
				if (ret == null) {
					ret = new FloatMessageQueue(config.getMerger(msgType));
					currentSG.put(p, ret);
				}
			}
		}
		return ret;
	}

	public int broadcastSubgraphSize() {
		int ret = 0;
		for (FloatMessageQueue s : readOnlySG.values())
			ret += s.size();
		return ret;
	}

}
