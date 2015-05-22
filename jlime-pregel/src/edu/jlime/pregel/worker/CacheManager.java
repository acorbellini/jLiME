package edu.jlime.pregel.worker;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.queues.DoubleMessageQueue;
import edu.jlime.pregel.queues.FloatMessageQueue;
import edu.jlime.pregel.queues.ObjectMessageQueue;
import edu.jlime.pregel.queues.PregelMessageQueue;
import edu.jlime.pregel.queues.SegmentedMessageQueue;

public class CacheManager {

	private Map<String, SegmentedMessageQueue> cache;

	private Map<String, PregelMessageQueue> cacheBroadcast;

	private WorkerTask task;
	private PregelConfig config;

	public CacheManager(WorkerTask workerTask, PregelConfig config) {
		this.task = workerTask;
		this.config = config;
		this.cache = new ConcurrentHashMap<>();
		this.cacheBroadcast = new ConcurrentHashMap<>();
	}

	public void flush() throws Exception {
		for (Entry<String, SegmentedMessageQueue> e : cache.entrySet()) {
			e.getValue().switchQueue();
			// TODO There must be a better way
			e.getValue().flush(e.getKey(), task);
		}
		for (Entry<String, PregelMessageQueue> e : cacheBroadcast.entrySet()) {
			e.getValue().switchQueue();
			e.getValue().flush(e.getKey(), task);
		}
	}

	private PregelMessageQueue getBroadcastCache(String type) {
		PregelMessageQueue ret = cacheBroadcast.get(type);
		if (ret == null) {
			synchronized (cacheBroadcast) {
				ret = cacheBroadcast.get(type);
				if (ret == null) {
					ret = task.getQueueFactory(type).getMQ();
					cacheBroadcast.put(type, ret);
				}
			}
		}
		return ret;
	}

	private SegmentedMessageQueue getCacheFor(String msgType) {
		SegmentedMessageQueue ret = cache.get(msgType);
		if (ret == null) {
			synchronized (cache) {
				ret = cache.get(msgType);
				if (ret == null) {
					ret = new SegmentedMessageQueue(msgType, task,
							config.getSegments(), config.getQueueSize(),
							task.getQueueFactory(msgType), config.getThreads());
					cache.put(msgType, ret);
				}
			}
		}
		return ret;
	}

	public void sendAll(String type, long from, Object msg) throws Exception {
		ObjectMessageQueue q = (ObjectMessageQueue) getBroadcastCache(type);
		synchronized (q) {
			checkBroadCacheSize(type, q);
			q.put(from, -1l, msg);
		}
	}

	private void checkBroadCacheSize(String type, PregelMessageQueue q)
			throws Exception {
		if (q.currentSize() == config.getBroadcastQueue()) {
			q.switchQueue();
			q.flush(type, task);
		}
	}

	public void send(String msgType, long from, long to, Object val)
			throws Exception {
		getCacheFor(msgType).put(from, to, val);
	}

	public void sendFloat(String msg, long from, long to, float val)
			throws Exception {
		getCacheFor(msg).putFloat(from, to, val);
	}

	public void sendAllFloat(String type, long from, float val)
			throws Exception {
		FloatMessageQueue q = (FloatMessageQueue) getBroadcastCache(type);
		synchronized (q) {
			checkBroadCacheSize(type, q);
			q.putFloat(from, -1l, val);
		}
	}

	public void sendAllDouble(String type, long from, double val)
			throws Exception {
		DoubleMessageQueue q = (DoubleMessageQueue) getBroadcastCache(type);
		synchronized (q) {
			checkBroadCacheSize(type, q);
			q.putDouble(from, -1l, val);
		}
	}

	public void sendDouble(String msg, long from, long to, double val)
			throws Exception {
		getCacheFor(msg).putDouble(from, to, val);
	}
}
