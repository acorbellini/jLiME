package edu.jlime.pregel.worker;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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

	private Map<String, PregelMessageQueue> cache;

	private Map<String, PregelMessageQueue> cacheBroadcast;

	private Map<Pair<String, String>, PregelMessageQueue> cacheBroadcastSubGraph;

	private WorkerTask task;
	private PregelConfig config;

	private Map<String, Future<?>> futures;

	private ExecutorService pool;

	private int max_size;

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
			this.pool = Executors.newFixedThreadPool(config.getThreads(),
					new ThreadFactory() {
						@Override
						public Thread newThread(Runnable r) {
							Thread t = Executors.defaultThreadFactory()
									.newThread(r);
							t.setName("Cache Sender Pool for Task "
									+ task.toString() + ", id:"
									+ task.getTaskid());
							t.setDaemon(true);
							return t;
						}
					});
		}
	}

	public void flush() throws Exception {
		if (parallel)
			for (Future<?> f : futures.values()) {
				if (f != null)
					f.get();
			}
		for (Entry<String, PregelMessageQueue> e : cache.entrySet()) {
			e.getValue().switchQueue();
			e.getValue().flush(e.getKey(), null, task);
		}
		for (Entry<String, PregelMessageQueue> e : cacheBroadcast.entrySet()) {
			e.getValue().switchQueue();
			e.getValue().flush(e.getKey(), null, task);
		}

		for (Entry<Pair<String, String>, PregelMessageQueue> e : cacheBroadcastSubGraph
				.entrySet()) {
			e.getValue().switchQueue();
			e.getValue().flush(e.getKey().left, e.getKey().right, task);
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

	private PregelMessageQueue getBroadcastSubgraphCache(Pair<String, String> p) {
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

	private PregelMessageQueue getCacheFor(String msgType) {
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

	public void send(String type, long from, long to, Object val)
			throws Exception {
		ObjectMessageQueue q = (ObjectMessageQueue) getCacheFor(type);
		synchronized (q) {
			checkSize(type, q);
			q.put(from, to, val);
		}
	}

	public void sendFloat(String type, long from, long to, float val)
			throws Exception {
		FloatMessageQueue q = (FloatMessageQueue) getCacheFor(type);
		synchronized (q) {
			checkSize(type, q);
			q.putFloat(from, to, val);
		}
	}

	public void sendDouble(String type, long from, long to, double val)
			throws Exception {
		DoubleMessageQueue q = (DoubleMessageQueue) getCacheFor(type);
		synchronized (q) {
			checkSize(type, q);
			q.putDouble(from, to, val);
		}
	}

	private void checkSize(final String type, final PregelMessageQueue cache)
			throws Exception {
		if (cache.currentSize() == max_size) {
			if (parallel) {
				Future<?> fut = futures.remove(type);
				if (fut != null)
					fut.get();
				cache.switchQueue();
				fut = pool.submit(new Runnable() {

					@Override
					public void run() {
						try {
							cache.flush(type, null, task);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});

				futures.put(type, fut);
			} else {
				cache.switchQueue();
				cache.flush(type, null, task);
			}

		}
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
			q.flush(type, null, task);
		}
	}

	@Override
	public void stop() {
		if (pool != null)
			pool.shutdown();
	}

	@Override
	public void sendAllSubGraph(String msgType, String subgraph, long v,
			Object val) throws Exception {
		Pair<String, String> p = new Pair<>(msgType, subgraph);
		ObjectMessageQueue q = (ObjectMessageQueue) getBroadcastSubgraphCache(p);
		synchronized (q) {
			checkBroadCacheSubgraphSize(p, q);
			q.put(v, -1l, val);
		}
	}

	private void checkBroadCacheSubgraphSize(Pair<String, String> p,
			PregelMessageQueue q) throws Exception {
		if (q.currentSize() == config.getBroadcastQueue()) {
			q.switchQueue();
			q.flush(p.left, p.right, task);
		}
	}

	@Override
	public void sendAllFloatSubGraph(String msgType, String subgraph, long v,
			float val) throws Exception {
		Pair<String, String> p = new Pair<>(msgType, subgraph);
		FloatMessageQueue q = (FloatMessageQueue) getBroadcastSubgraphCache(p);
		synchronized (q) {
			checkBroadCacheSubgraphSize(p, q);
			q.putFloat(v, -1l, val);
		}
	}
}
