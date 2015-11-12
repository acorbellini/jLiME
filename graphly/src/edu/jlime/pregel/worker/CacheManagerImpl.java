package edu.jlime.pregel.worker;

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.queues.DoubleMessageQueue;
import edu.jlime.pregel.queues.FloatMessageQueue;
import edu.jlime.pregel.queues.ObjectMessageQueue;
import edu.jlime.pregel.queues.PregelMessageQueue;
import edu.jlime.util.Pair;

public class CacheManagerImpl implements CacheManagerI {

	private static final UUID BROADCAST_UUID = new UUID(0, 0);

	private Map<String, PregelMessageQueue> cache;

	private Map<String, PregelMessageQueue> cacheBroadcast;

	private Map<Pair<String, String>, PregelMessageQueue> cacheBroadcastSubGraph;

	private WorkerTask task;
	private PregelConfig config;

	private Map<String, Future<?>> futures;

	private ExecutorService pool;

	private float max_size;

	private boolean parallel;

	public CacheManagerImpl(WorkerTask workerTask, PregelConfig config) {
		this.task = workerTask;
		this.max_size = config.getCacheSize();
		this.config = config;
		this.cache = new ConcurrentHashMap<>();
		this.cacheBroadcast = new ConcurrentHashMap<>();
		this.cacheBroadcastSubGraph = new ConcurrentHashMap<>();

		this.parallel = config.isParallelCache();
		if (parallel) {
			this.futures = new ConcurrentHashMap<>();
			this.pool = Executors.newFixedThreadPool(config.getThreads(), new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("Cache Sender Pool for Task " + task.toString() + ", id:" + task.getTaskid());
					t.setDaemon(true);
					return t;
				}
			});
		}
	}

	public void flush() throws Exception {
		waitParallel();
		for (Entry<String, PregelMessageQueue> e : cache.entrySet())
			e.getValue().flush(e.getKey(), null, task);
		for (Entry<String, PregelMessageQueue> e : cacheBroadcast.entrySet())
			e.getValue().flush(e.getKey(), null, task);

		for (Entry<Pair<String, String>, PregelMessageQueue> e : cacheBroadcastSubGraph.entrySet())
			e.getValue().flush(e.getKey().left, e.getKey().right, task);
	}

	public PregelMessageQueue getBroadcastCache(String type) {
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

	public PregelMessageQueue getBroadcastSubgraphCache(Pair<String, String> p) {
		PregelMessageQueue ret = cacheBroadcastSubGraph.get(p);
		if (ret == null) {
			synchronized (cacheBroadcastSubGraph) {
				ret = cacheBroadcastSubGraph.get(p);
				if (ret == null) {
					ret = task.getQueueFactory(p.left).getMQ();
					cacheBroadcastSubGraph.put(p, ret);
				}
			}
		}
		return ret;
	}

	public PregelMessageQueue getCacheFor(String msgType) {
		PregelMessageQueue ret = cache.get(msgType);
		if (ret == null) {
			synchronized (cache) {
				ret = cache.get(msgType);
				if (ret == null) {
					ret = task.getQueueFactory(msgType).getMQ();
					cache.put(msgType, ret);
				}
			}
		}
		return ret;
	}

	public void send(String type, long from, long to, Object val) throws Exception {
		ObjectMessageQueue q = (ObjectMessageQueue) getCacheFor(type);
		q.put(from, to, val);
	}

	public void sendFloat(UUID wID, String type, long from, long to, float val) throws Exception {
		FloatMessageQueue q = (FloatMessageQueue) getCacheFor(type);
		q.putFloat(wID, from, to, val);
	}

	public void sendDouble(String type, long from, long to, double val) throws Exception {
		DoubleMessageQueue q = (DoubleMessageQueue) getCacheFor(type);
		q.putDouble(from, to, val);
	}

	public void sendAllFloat(String type, long from, float val) throws Exception {
		FloatMessageQueue q = (FloatMessageQueue) getBroadcastCache(type);
		q.putFloat(BROADCAST_UUID, from, -1l, val);
	}

	public void sendAllDouble(String type, long from, double val) throws Exception {
		DoubleMessageQueue q = (DoubleMessageQueue) getBroadcastCache(type);
		q.putDouble(from, -1l, val);
	}

	public void sendAll(String type, long from, Object msg) throws Exception {
		ObjectMessageQueue q = (ObjectMessageQueue) getBroadcastCache(type);
		q.put(from, -1l, msg);
	}

	@Override
	public void stop() {
		if (pool != null)
			pool.shutdown();
	}

	@Override
	public void sendAllSubGraph(String msgType, String subgraph, long v, Object val) throws Exception {
		Pair<String, String> p = new Pair<>(msgType, subgraph);
		ObjectMessageQueue q = (ObjectMessageQueue) getBroadcastSubgraphCache(p);
		q.put(v, -1l, val);
	}

	@Override
	public void sendAllFloatSubGraph(String msgType, String subgraph, long v, float val) throws Exception {
		Pair<String, String> p = new Pair<>(msgType, subgraph);
		FloatMessageQueue q = (FloatMessageQueue) getBroadcastSubgraphCache(p);
		q.putFloat(BROADCAST_UUID, v, -1l, val);
	}

	@Override
	public void mergeWith(CacheManagerI cmi) throws Exception {
		waitParallel();

		CacheManagerImpl other = (CacheManagerImpl) cmi;

		for (Entry<String, PregelMessageQueue> e : cache.entrySet()) {
			PregelMessageQueue pmq = other.getCacheFor(e.getKey());
			e.getValue().transferTo(pmq);
		}
		for (Entry<String, PregelMessageQueue> e : cacheBroadcast.entrySet()) {
			PregelMessageQueue pmq = other.getBroadcastCache(e.getKey());
			e.getValue().transferTo(pmq);
		}

		for (Entry<Pair<String, String>, PregelMessageQueue> e : cacheBroadcastSubGraph.entrySet()) {
			PregelMessageQueue pmq = other.getBroadcastSubgraphCache(e.getKey());
			e.getValue().transferTo(pmq);
		}
	}

	private void waitParallel() throws InterruptedException, ExecutionException {
		if (parallel)
			for (Future<?> f : futures.values()) {
				if (f != null)
					f.get();
			}
	}
}
