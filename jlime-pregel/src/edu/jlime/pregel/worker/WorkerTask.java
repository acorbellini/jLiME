package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.PregelSubgraph;
import edu.jlime.pregel.client.CoordinatorFilter;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.mergers.MessageMerger;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.queues.DoubleData;
import edu.jlime.pregel.queues.DoubleMessageQueue;
import edu.jlime.pregel.queues.FloatArrayData;
import edu.jlime.pregel.queues.FloatArrayMessageQueue;
import edu.jlime.pregel.queues.FloatMessageQueue;
import edu.jlime.pregel.queues.MessageQueueFactory;
import edu.jlime.pregel.queues.ObjectData;
import edu.jlime.pregel.queues.ObjectMessageQueue;
import edu.jlime.pregel.queues.PregelMessageQueue;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class WorkerTask {

	protected Logger log = Logger.getLogger(WorkerTask.class);

	Map<String, Aggregator> aggregators = new HashMap<>();

	private Worker[] workers;

	private HashMap<Integer, CacheManagerI> cacheMgr;

	private Graph graph;

	private int taskid;

	private VertexFunction f;

	private WorkerImpl worker;

	private int currentStep;

	private PregelConfig config;

	private Map<String, PregelMessageQueue> queue;

	private Map<String, PregelMessageQueue> broadcast;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	private SplitFunction split;

	private ExecutorService vertexPool;

	private ExecutorService multiSendPool;

	private Semaphore multiSendCounter;

	private AtomicInteger sendCount = new AtomicInteger(0);

	// private VertexList vList;

	private VertexList graphVertexList;

	// private VertexList currentSplit;

	private AtomicInteger vertexCounter = new AtomicInteger(0);

	private Map<Pair<String, String>, PregelMessageQueue> subgraphqueue;

	private Map<String, PregelSubgraph> subgraphs = new ConcurrentHashMap<>();

	public WorkerTask(WorkerImpl w, RPCDispatcher rpc, Peer client,
			final VertexFunction func, long[] vList, final int taskID,
			PregelConfig config) throws Exception {
		this.log.info("Creating task with ID " + taskID);
		this.graph = config.getGraph().getGraph(rpc);
		for (Entry<String, Aggregator> e : config.getAggregators().entrySet()) {
			aggregators.put(e.getKey(), e.getValue().copy());
		}

		this.cacheMgr = new HashMap<>();
		for (int i = 0; i < config.getThreads(); i++) {
			this.cacheMgr.put(i, config.getCacheFactory().build(this, config));
		}

		for (Entry<String, TLongHashSet> e : config.getSubgraphs().entrySet()) {
			subgraphs.put(e.getKey(), new PregelSubgraph(e.getValue(), graph));
		}

		// this.vList = config.isPersitentVertexList() ? new
		// PersistedVertexList()
		// : new InMemVertexList();

		// this.currentSplit = config.isPersitentCurrentSplitList() ? new
		// PersistedVertexList()
		// : new InMemVertexList();

		this.vertexPool = Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("Vertex Pool for Task " + func.toString() + ", id:"
						+ taskID);
				t.setDaemon(true);
				return t;
			}
		});

		this.multiSendPool = Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("Grouped Send Pool for Task " + func.toString()
						+ ", id:" + taskID);
				t.setDaemon(true);
				return t;
			}
		});
		this.multiSendCounter = new Semaphore(config.getSendThreads());

		this.worker = w;

		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc,
				CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				client);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new WorkerFilter(), client);

		this.taskid = taskID;

		// if (config.isExecuteOnAll()) {
		// long start = System.currentTimeMillis();
		// log.info("Creating vertex list for the whole graph.");
		// for (Long vid : graph.vertices()) {
		// this.vList.add(vid);
		// }
		// log.info("Finished creating vertex list for the whole graph in "
		// + (System.currentTimeMillis() - start) / 1000f + " sec.");
		// } else {
		// for (long vid : vList) {
		// this.vList.add(vid);
		// }
		// }

		// this.vList.close();

		this.config = config;
		this.queue = new ConcurrentHashMap<>();
		this.broadcast = new ConcurrentHashMap<>();
		this.subgraphqueue = new ConcurrentHashMap<>();

		this.f = func;
	}

	public void queueVertexData(String msg, long from, long to, Object val)
			throws Exception {
		((ObjectMessageQueue) getQueue(msg)).put(from, to, val);
	}

	private PregelMessageQueue getQueue(String msg) {
		PregelMessageQueue ret = queue.get(msg);
		if (ret == null) {
			synchronized (queue) {
				ret = queue.get(msg);
				if (ret == null) {
					// ret = new SegmentedMessageQueue(msg, this,
					// config.getSegments(), Integer.MAX_VALUE,
					// getQueueFactory(msg), config.getThreads());
					ret = getQueueFactory(msg).getMQ();
					queue.put(msg, ret);
				}
			}
		}
		return ret;
	}

	MessageQueueFactory getQueueFactory(String msgType) {
		MessageMerger merger = config.getMerger(msgType);
		MessageQueueFactory fact = merger != null ? merger.getFactory()
				: MessageQueueFactory.simple(null);
		return fact;
	}

	public void nextStep(int superstep, SplitFunction func,
			Map<String, Aggregator> aggregators) throws Exception {
		this.log.info("Configuring step " + superstep + " on Worker "
				+ worker.getID());
		for (Entry<String, Aggregator> e : aggregators.entrySet()) {
			this.aggregators.put(e.getKey(), e.getValue().copy());
		}

		this.split = func;

		Peer[] peers = this.split.getPeers();

		this.workers = new Worker[peers.length];
		for (int i = 0; i < peers.length; i++)
			this.workers[i] = this.workerMgr.get(peers[i]);

		this.currentStep = superstep;

		for (PregelMessageQueue s : queue.values())
			s.switchQueue();

		for (PregelMessageQueue s : broadcast.values())
			s.switchQueue();

		for (PregelMessageQueue s : subgraphqueue.values())
			s.switchQueue();

		// TODO It's done twice, called with -1 and 0.
		// this.log.info("Loading local slice of vertices from " + vList.size()
		// + " vertices.");

		// this.currentSplit.delete();
		// this.currentSplit = this.config.isPersitentCurrentSplitList() ? new
		// PersistedVertexList()
		// : new InMemVertexList();

		// Peer localPeer = this.workerMgr.getLocalPeer();
		// List<Peer> peers2 = this.workerMgr.getPeers();

		// LongIterator it = vList.iterator();
		// while (it.hasNext()) {
		// long vid = it.next();
		// if (this.split.getPeer(vid, peers2).equals(localPeer))
		// this.currentSplit.add(vid);
		// }
		// this.currentSplit.close();
		// this.log.info("Loaded slice " + this.currentSplit.size() +
		// " vertices.");
	}

	public void execute() throws Exception {
		long init = System.currentTimeMillis();
		final VertexList vList = this.config.isPersitentCurrentSplitList() ? new PersistedVertexList()
				: new InMemVertexList();

		if (broadcastSize() > 0) { // activate all vertices.
			if (this.graphVertexList == null) {
				this.graphVertexList = this.config.isPersitentVertexList() ? new PersistedVertexList()
						: new InMemVertexList();
				Iterable<Long> vertices = graph.vertices();
				for (Long v : vertices) {
					graphVertexList.add(v);
				}
				graphVertexList.close();
			}

			Peer localPeer = this.workerMgr.getLocalPeer();
			List<Peer> peers2 = this.workerMgr.getPeers();

			LongIterator it = graphVertexList.iterator();
			while (it.hasNext()) {
				long vid = it.next();
				if (this.split.getPeer(vid, peers2).equals(localPeer))
					vList.add(vid);
			}
			this.log.info("Loaded slice " + vList.size() + " vertices.");
		} else {
			TLongHashSet activeVertices = new TLongHashSet();
			if (subgraphSize() > 0) {
				Peer localPeer = this.workerMgr.getLocalPeer();
				List<Peer> peers2 = this.workerMgr.getPeers();
				for (Entry<String, TLongHashSet> e : config.getSubgraphs()
						.entrySet()) {
					TLongIterator it = e.getValue().iterator();
					while (it.hasNext()) {
						long vid = it.next();
						if (this.split.getPeer(vid, peers2).equals(localPeer))
							activeVertices.add(vid);
					}
				}
			}

			for (Entry<String, PregelMessageQueue> e : queue.entrySet()) {
				activeVertices.addAll(e.getValue().keys());
			}
			TLongIterator it = activeVertices.iterator();
			while (it.hasNext())
				vList.add(it.next());
		}
		vList.close();

		final int size = vList.size();

		if (size == 0) {
			this.log.info("Queue was empty, finished step " + currentStep
					+ " on Worker " + this.worker.getID());
			this.coordMgr.getFirst().finished(getTaskid(), this.worker.getID(),
					false, aggregators);
		} else {

			this.log.info("Starting Execution on worker " + worker.getID()
					+ " for " + size + " messages.");

			final int threads = this.config.getThreads();

			final AtomicInteger count = new AtomicInteger(0);
			for (int i = 0; i < this.config.getThreads(); i++) {
				final int currentIndex = i;
				this.vertexCounter.incrementAndGet();
				this.vertexPool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							executeVertex(size, threads, count, currentIndex,
									vList);
						} catch (Exception e) {
							e.printStackTrace();
						}

						int vc = vertexCounter.decrementAndGet();
						if (vc == 0)
							synchronized (vertexCounter) {
								vertexCounter.notify();
							}
					}
				});
			}

			synchronized (vertexCounter) {
				while (vertexCounter.get() != 0)
					vertexCounter.wait();
			}
			if (!config.isBSPMode()) {
				flushCaches();
			}
			log.info("Finished work for step " + currentStep + " on Worker "
					+ worker.getID());
			coordMgr.getFirst().finished(getTaskid(), this.worker.getID(),
					true, aggregators);

			vList.delete();
		}

		log.info("Completed step in " + (System.currentTimeMillis() - init));
	}

	private void flushCaches() throws Exception, InterruptedException {
		log.info("Flushing cache on step " + currentStep + " on Worker "
				+ worker.getID());
		for (CacheManagerI cache : cacheMgr.values()) {
			cache.flush();
		}

		synchronized (sendCount) {
			while (sendCount.get() != 0)
				sendCount.wait();
		}
	}

	private int broadcastSize() {
		int ret = 0;
		for (PregelMessageQueue s : broadcast.values())
			ret += s.readOnlySize();
		return ret;
	}

	private int subgraphSize() {
		int ret = 0;
		for (PregelMessageQueue s : subgraphqueue.values())
			ret += s.readOnlySize();
		return ret;
	}

	private void executeVertex(final int size, final int threads,
			final AtomicInteger count, final int threadID, VertexList vList)
			throws Exception {
		int vertexCursor = 0;

		LongIterator it = vList.iterator();

		ArrayList<Iterator<PregelMessage>> currList = new ArrayList<>();

		WorkerContext ctx = new WorkerContext(WorkerTask.this,
				cacheMgr.get(threadID), -1);

		while (it.hasNext()) {
			long currentVertex = it.next();
			if (vertexCursor % threads == threadID) {
				ctx.setCurrVertex(currentVertex);
				currList.clear();
				for (Entry<String, PregelMessageQueue> e : queue.entrySet()) {
					PregelMessageQueue q = e.getValue();
					currList.add(q.getMessages(e.getKey(), currentVertex));
				}

				for (Entry<String, PregelMessageQueue> e : broadcast.entrySet()) {
					PregelMessageQueue q = e.getValue();
					currList.add(q.getMessages(e.getKey(), -1l));
				}

				for (Entry<Pair<String, String>, PregelMessageQueue> e : subgraphqueue
						.entrySet()) {
					PregelMessageQueue q = e.getValue();
					if (config.getSubgraph(e.getKey().right).contains(
							currentVertex))
						currList.add(q.getMessages(e.getKey().left, -1l));
				}

				Iterator<PregelMessage> messages = new ConcatIterator(currList);
				if (messages.hasNext()) {
					printCompleted(size, count.getAndIncrement());

					if (log.isDebugEnabled())
						log.debug("Executing function on vertex "
								+ currentVertex);

					f.execute(currentVertex, messages, ctx);

					if (log.isDebugEnabled())
						log.debug("Finished executing function on vertex "
								+ currentVertex);
				}

			}
			vertexCursor++;
		}
	}

	private void printCompleted(int total, double currentCount)
			throws Exception {
		int fraction = ((int) (total / (double) 10));
		if ((currentCount % fraction) == 0) {
			double completed = ((currentCount / total) * 100);
			log.info("Completed work on worker " + worker.getID() + ": "
					+ Math.ceil(completed) + " % ");
		}
	}

	public Graph getGraph() {
		return graph;
	}

	public Worker getWorker() {
		return worker;
	}

	public Worker getWorker(long vid) {
		return workers[split.hash(vid)];
	}

	public Integer getSuperStep() {
		return currentStep;
	}

	public PregelConfig getConfig() {
		return config;
	}

	public void cleanup() throws Exception {
		vertexPool.shutdown();
		multiSendPool.shutdown();
		for (Entry<Integer, CacheManagerI> e : cacheMgr.entrySet()) {
			e.getValue().stop();
		}
	}

	public void outputDouble(String msgtype, long from, long to, double val)
			throws Exception {
		if (to != -1l)
			getWorker(to)
					.sendDoubleMessage(msgtype, from, to, val, getTaskid());
		else
			workerMgr.broadcast().sendDoubleBroadcastMessage(msgtype, from,
					val, getTaskid());
	}

	public void outputObject(String msgtype, long from, long to, Object val)
			throws Exception {
		if (to != -1l)
			getWorker(to).sendMessage(msgtype, from, to, val, getTaskid());
		else
			workerMgr.broadcast().sendBroadcastMessage(msgtype, from, val,
					getTaskid());
	}

	public void outputFloat(String msgtype, long from, long to, float value)
			throws Exception {
		if (to != -1l)
			getWorker(to).sendFloatMessage(msgtype, from, to, value,
					getTaskid());
		else
			workerMgr.broadcast().sendFloatBroadcastMessage(msgtype, from,
					value, getTaskid());
	}

	public int getTaskid() {
		return taskid;
	}

	public void sendFloats(final String msgType, HashMap<Worker, FloatData> ret)
			throws Exception {
		for (final Entry<Worker, FloatData> e : ret.entrySet()) {
			multiSendCounter.acquire();
			sendCount.incrementAndGet();
			multiSendPool.execute(new Runnable() {
				@Override
				public void run() {
					FloatData fData = e.getValue();
					try {
						e.getKey().sendFloatMessage(msgType, -1l, fData.keys,
								fData.values, taskid);
					} catch (Exception e1) {
						e1.printStackTrace();
					} finally {
						sendCount.decrementAndGet();
						synchronized (sendCount) {
							sendCount.notify();
						}
						multiSendCounter.release();
					}
				}
			});
		}

	}

	public void sendDoubles(final String msgType,
			HashMap<Worker, DoubleData> ret) throws Exception {
		for (final Entry<Worker, DoubleData> e : ret.entrySet()) {
			multiSendCounter.acquire();
			sendCount.incrementAndGet();
			multiSendPool.execute(new Runnable() {
				@Override
				public void run() {
					DoubleData dData = e.getValue();
					try {
						e.getKey().sendDoubleMessage(msgType, -1l, dData.keys,
								dData.values, taskid);
					} catch (Exception e1) {
						e1.printStackTrace();
					} finally {
						sendCount.decrementAndGet();
						synchronized (sendCount) {
							sendCount.notify();
						}
						multiSendCounter.release();
					}
				}
			});
		}
	}

	public void queueFloatVertexData(String msg, long from, long to, float val)
			throws Exception {
		((FloatMessageQueue) getQueue(msg)).putFloat(from, to, val);
	}

	public void queueDoubleVertexData(String msg, long from, long to, double val)
			throws Exception {
		((DoubleMessageQueue) getQueue(msg)).putDouble(from, to, val);
	}

	public void queueBroadcastDoubleVertexData(String type, long from,
			double val) throws Exception {
		DoubleMessageQueue q = (DoubleMessageQueue) getBroadcastQueue(type);
		q.putDouble(from, -1l, val);
	}

	private PregelMessageQueue getBroadcastQueue(String msg) {
		PregelMessageQueue ret = broadcast.get(msg);
		if (ret == null) {
			synchronized (broadcast) {
				ret = broadcast.get(msg);
				if (ret == null) {
					ret = getQueueFactory(msg).getMQ();
					broadcast.put(msg, ret);
				}
			}
		}
		return ret;
	}

	public void queueBroadcastVertexData(String msg, long from, Object val)
			throws Exception {
		ObjectMessageQueue q = (ObjectMessageQueue) getBroadcastQueue(msg);
		q.put(from, -1, val);
	}

	public void queueBroadcastFloatVertexData(String msg, long from, float val)
			throws Exception {
		FloatMessageQueue q = (FloatMessageQueue) getBroadcastQueue(msg);
		q.putFloat(from, -1l, val);
	}

	public Aggregator getAggregator(String string) {
		return aggregators.get(string);
	}

	public void outputFloatArray(String msgtype, long from, long to,
			float[] value) throws Exception {
		if (to != -1l)
			getWorker(to).sendFloatArrayMessage(msgtype, from, to, value,
					getTaskid());
		else
			workerMgr.broadcast().sendFloatArrayBroadcastMessage(msgtype, from,
					value, getTaskid());
	}

	public void sendFloatArrays(final String msgType,
			HashMap<Worker, FloatArrayData> ret) throws InterruptedException {
		for (final Entry<Worker, FloatArrayData> e : ret.entrySet()) {
			multiSendCounter.acquire();
			sendCount.incrementAndGet();
			multiSendPool.execute(new Runnable() {
				@Override
				public void run() {
					FloatArrayData fData = e.getValue();
					try {
						e.getKey().sendFloatArrayMessage(msgType, -1l,
								fData.getVids(), fData.getData(), taskid);
					} catch (Exception e1) {
						e1.printStackTrace();
					} finally {
						sendCount.decrementAndGet();
						synchronized (sendCount) {
							sendCount.notify();
						}
						multiSendCounter.release();
					}
				}
			});
		}
	}

	public void queueBroadcastFloatArrayVertexData(String msg, long from,
			float[] val) throws Exception {
		FloatArrayMessageQueue q = (FloatArrayMessageQueue) getBroadcastQueue(msg);
		q.putFloatArray(from, -1l, val);
	}

	public void queueFloatArrayVertexData(String msg, long from, long to,
			float[] data) throws Exception {
		((FloatArrayMessageQueue) getQueue(msg)).putFloatArray(from, to, data);
	}

	public void sendObjects(final String msgType,
			HashMap<Worker, ObjectData> ret) throws InterruptedException {
		for (final Entry<Worker, ObjectData> e : ret.entrySet()) {
			multiSendCounter.acquire();
			sendCount.incrementAndGet();
			multiSendPool.execute(new Runnable() {
				@Override
				public void run() {
					ObjectData dData = e.getValue();
					try {
						e.getKey().sendObjectsMessage(msgType, dData.from,
								dData.vids, dData.objects, taskid);
					} catch (Exception e1) {
						e1.printStackTrace();
					} finally {
						sendCount.decrementAndGet();
						synchronized (sendCount) {
							sendCount.notify();
						}
						multiSendCounter.release();
					}
				}
			});
		}
	}

	public void queueVertexData(String msgtype, long[] from, long[] to,
			Object[] objects) {
		ObjectMessageQueue q = (ObjectMessageQueue) getQueue(msgtype);
		synchronized (q) {
			for (int i = 0; i < to.length; i++) {
				q.put(from != null ? from[i] : -1l, to[i],
						objects == null ? null : objects[i]);
			}
		}
	}

	public void queueFloatVertexData(String msgType, long from, long[] to,
			float[] vals) {
		FloatMessageQueue q = (FloatMessageQueue) getQueue(msgType);
		synchronized (q) {
			for (int i = 0; i < to.length; i++) {
				q.putFloat(from, to[i], vals[i]);
			}
		}
	}

	public void outputObjectSubgraph(String msgType, String subGraph, long v,
			Object val) throws Exception {
		workerMgr.broadcast().sendBroadcastMessageSubgraph(msgType, subGraph,
				v, val, getTaskid());
	}

	public void queueBroadcastSubgraphVertexData(String msgType,
			String subGraph2, Object val) {
		ObjectMessageQueue q = (ObjectMessageQueue) getBroadcastSubgraphQueue(
				msgType, subGraph2);
		q.put(-1, -1, val);
	}

	private PregelMessageQueue getBroadcastSubgraphQueue(String msgType,
			String subGraph2) {
		Pair<String, String> p = new Pair<>(msgType, subGraph2);
		PregelMessageQueue ret = subgraphqueue.get(p);
		if (ret == null) {
			synchronized (subgraphqueue) {
				ret = subgraphqueue.get(p);
				if (ret == null) {
					ret = getQueueFactory(p.left).getMQ();
					subgraphqueue.put(p, ret);
				}
			}
		}
		return ret;
	}

	public PregelSubgraph getSubgraph(String string) {
		return subgraphs.get(string);
	}

	public void outputFloatSubgraph(String msgType, String subgraph, long from,
			float val) throws Exception {
		workerMgr.broadcast().sendBroadcastMessageSubgraphFloat(msgType,
				subgraph, from, val, getTaskid());
	}

	public void queueBroadcastSubgraphFloat(String msgType, String subgraph,
			float val) {
		FloatMessageQueue q = (FloatMessageQueue) getBroadcastSubgraphQueue(
				msgType, subgraph);
		q.putFloat(-1, -1, val);
	}

	public boolean isLocal(long to) {
		return getWorker(to).equals(workerMgr.get(workerMgr.getLocalPeer()));
	}

	public void finishedProcessing() throws Exception {
		if (config.isBSPMode()) {
			long init = System.currentTimeMillis();
			flushCaches();
			log.info("Finished flushing in BSP mode: "
					+ (System.currentTimeMillis() - init) + " ms");
		}
	}
}
