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
import edu.jlime.pregel.queues.FloatArrayData;
import edu.jlime.pregel.queues.MessageQueueFactory;
import edu.jlime.pregel.queues.SegmentedMessageQueue;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;

public class WorkerTask {

	protected Logger log = Logger.getLogger(WorkerTask.class);

	HashMap<String, Aggregator> aggregators = new HashMap<>();

	private Worker[] workers;

	private HashMap<Integer, CacheManager> cacheMgr;

	private Graph graph;

	private int taskid;

	private VertexFunction f;

	private WorkerImpl worker;

	private int currentStep;

	private PregelConfig config;

	private Map<String, SegmentedMessageQueue> queue;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	private SplitFunction split;

	private ExecutorService vertexPool;

	private ExecutorService multiSendPool;

	private Semaphore multiSendCounter;

	private AtomicInteger sendCount = new AtomicInteger(0);

	private VertexList vList;

	private VertexList currentSplit;

	private AtomicInteger vertexCounter = new AtomicInteger(0);

	public WorkerTask(WorkerImpl w, RPCDispatcher rpc, Peer client,
			final VertexFunction func, long[] vList, final int taskID,
			PregelConfig config) throws Exception {
		this.log.info("Creating task with ID " + taskID);

		for (Entry<String, Aggregator> e : config.getAggregators().entrySet()) {
			aggregators.put(e.getKey(), e.getValue().copy());
		}

		this.cacheMgr = new HashMap<>();
		for (int i = 0; i < config.getThreads(); i++) {
			this.cacheMgr.put(i, new CacheManager(this, config));
		}

		this.vList = config.isPersitentVertexList() ? new PersistedVertexList()
				: new InMemVertexList();

		this.currentSplit = config.isPersitentCurrentSplitList() ? new PersistedVertexList()
				: new InMemVertexList();

		this.vertexPool = Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("Vertex Pool for Task " + func.toString() + ", id:"
						+ taskID);
				return t;
			}
		});

		this.multiSendPool = Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("Grouped Send Pool for Task " + func.toString()
						+ ", id:" + taskID);
				return t;
			}
		});
		this.multiSendCounter = new Semaphore(config.getSendThreads());

		this.graph = config.getGraph().getGraph(rpc);

		this.worker = w;

		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc,
				CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				client);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new WorkerFilter(), client);

		this.taskid = taskID;

		if (config.isExecuteOnAll()) {
			long start = System.currentTimeMillis();
			log.info("Creating vertex list for the whole graph.");
			for (Long vid : graph.vertices()) {
				this.vList.add(vid);
			}
			this.vList.flush();
			log.info("Finished creating vertex list for the whole graph in "
					+ (System.currentTimeMillis() - start) / 1000f + " sec.");
		} else {
			for (long vid : vList) {
				this.vList.add(vid);
			}
		}

		this.config = config;
		this.queue = new ConcurrentHashMap<>();

		this.f = func;
	}

	public void queueVertexData(String msg, long from, long to, Object val)
			throws Exception {
		getQueue(msg).put(from, to, val);
	}

	private SegmentedMessageQueue getQueue(String msg) {
		SegmentedMessageQueue ret = queue.get(msg);
		if (ret == null) {
			synchronized (queue) {
				ret = queue.get(msg);
				if (ret == null) {
					ret = new SegmentedMessageQueue(msg, this,
							config.getSegments(), Integer.MAX_VALUE,
							getQueueFactory(msg), config.getThreads());
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

	public void nextStep(int superstep, SplitFunction func) throws Exception {
		this.log.info("Configuring step " + superstep + " on Worker "
				+ worker.getID());

		for (Entry<String, Aggregator> e : aggregators.entrySet()) {
			e.getValue().reset();
		}

		this.split = func;

		Peer[] peers = this.split.getPeers();

		this.workers = new Worker[peers.length];
		for (int i = 0; i < peers.length; i++)
			this.workers[i] = this.workerMgr.get(peers[i]);

		this.currentStep = superstep;

		for (SegmentedMessageQueue s : queue.values())
			s.switchQueue();

		// TODO It's done twice, called with -1 and 0.
		this.log.info("Loading local slice of vertices from " + vList.size()
				+ " vertices.");

		this.currentSplit.delete();
		this.currentSplit = this.config.isPersitentCurrentSplitList() ? new PersistedVertexList()
				: new InMemVertexList();

		Peer localPeer = this.workerMgr.getLocalPeer();
		List<Peer> peers2 = this.workerMgr.getPeers();

		LongIterator it = vList.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			if (this.split.getPeer(vid, peers2).equals(localPeer))
				this.currentSplit.add(vid);
		}
		this.currentSplit.flush();
		this.log.info("Loaded slice " + this.currentSplit.size() + " vertices.");
	}

	public void execute() throws Exception {
		final int size = queueSize();

		if (size == 0) {
			this.log.info("Queue was empty, finished step " + currentStep
					+ " on Worker " + this.worker.getID());
			this.coordMgr.getFirst().finished(getTaskid(), this.worker.getID(),
					false, aggregators);
			return;
		}

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
						executeVertex(size, threads, count, currentIndex);
					} catch (Exception e) {
						e.printStackTrace();
					}

					vertexCounter.decrementAndGet();
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

		synchronized (sendCount) {
			while (sendCount.get() != 0)
				sendCount.wait();
		}

		log.info("Flushing cache on step " + currentStep + " on Worker "
				+ worker.getID());
		for (CacheManager cache : cacheMgr.values()) {
			cache.flush();
		}

		log.info("Finished work for step " + currentStep + " on Worker "
				+ worker.getID());
		coordMgr.getFirst().finished(getTaskid(), this.worker.getID(), true,
				aggregators);
	}

	private void executeVertex(final int size, final int threads,
			final AtomicInteger count, final int threadID) throws Exception {
		int vertexCursor = 0;

		LongIterator it = currentSplit.iterator();

		ArrayList<Iterator<PregelMessage>> currList = new ArrayList<>();

		WorkerContext ctx = new WorkerContext(WorkerTask.this,
				cacheMgr.get(threadID), -1);

		while (it.hasNext()) {
			long currentVertex = it.next();
			if (vertexCursor % threads == threadID) {
				ctx.setCurrVertex(currentVertex);
				currList.clear();
				for (Entry<String, SegmentedMessageQueue> e : queue.entrySet()) {
					SegmentedMessageQueue q = e.getValue();
					currList.add(q.getMessages(e.getKey(), currentVertex));
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

	private int queueSize() {
		int ret = 0;
		for (SegmentedMessageQueue s : queue.values())
			ret += s.readOnlySize();
		return ret;
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

	public Worker getWorker(long vid) {
		return workers[split.hash(vid)];
	}

	public Integer getSuperStep() {
		return currentStep;
	}

	// public Double getAggregatedValue(Long v, String k) throws Exception {
	// return coordMgr.getFirst().getAggregatedValue(getTaskid(), v, k);
	// }
	//
	// public void setAggregatedValue(Long v, String string, double currentVal)
	// throws Exception {
	// coordMgr.getFirst().setAggregatedValue(getTaskid(), v, string,
	// currentVal);
	// }

	public PregelConfig getConfig() {
		return config;
	}

	public void cleanup() throws Exception {
		vertexPool.shutdown();
		multiSendPool.shutdown();
		currentSplit.delete();
		vList.delete();
	}

	// public void send(String msgType, long from, long to, Object val)
	// throws Exception {
	// cacheMgr.send(msgType, from, to, val);
	// }
	//
	// public void sendFloat(String msg, long from, long to, float val)
	// throws Exception {
	// cacheMgr.sendFloat(msg, from, to, val);
	// }
	//
	// public void sendDouble(String msg, long from, long to, double val)
	// throws Exception {
	// cacheMgr.sendDouble(msg, from, to, val);
	// }
	//
	// public void sendAll(String type, long from, Object msg) throws Exception
	// {
	// cacheMgr.sendAll(type, from, msg);
	// }
	//
	// public void sendAllFloat(String type, long from, float val)
	// throws Exception {
	// cacheMgr.sendAllFloat(type, from, val);
	// }
	//
	// public void sendAllDouble(String type, long from, double val)
	// throws Exception {
	// cacheMgr.sendAllDouble(type, from, val);
	// }

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
						multiSendCounter.release();
					}
				}
			});
		}
	}

	public void queueDoubleVertexData(String msg, long from, long to, double val)
			throws Exception {
		// DoubleAggregator ag = (DoubleAggregator) aggregators.get(msg);
		// if (ag != null)
		// ag.add(from, to, val);
		getQueue(msg).putDouble(from, to, val);
	}

	public void queueBroadcastDoubleVertexData(String type, long from,
			double val) throws Exception {
		// DoubleAggregator ag = (DoubleAggregator) aggregators.get(type);

		SegmentedMessageQueue q = getQueue(type);
		LongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			// if (ag != null)
			// ag.add(from, vid, val);

			q.putDouble(from, vid, val);
		}
	}

	public void queueFloatVertexData(String msg, long from, long to, float val)
			throws Exception {
		// FloatAggregator ag = (FloatAggregator) aggregators.get(msg);
		// if (ag != null)
		// ag.add(from, to, val);

		getQueue(msg).putFloat(from, to, val);
	}

	public void queueBroadcastVertexData(String msg, long from, Object val)
			throws Exception {

		// GenericAggregator ag = (GenericAggregator) aggregators.get(msg);

		SegmentedMessageQueue q = getQueue(msg);
		LongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();

			// if (ag != null)
			// ag.add(from, vid, val);

			q.put(from, vid, val);
		}
	}

	public void queueBroadcastFloatVertexData(String msg, long from, float val)
			throws Exception {
		// FloatAggregator ag = (FloatAggregator) aggregators.get(msg);

		SegmentedMessageQueue q = getQueue(msg);
		LongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			// if (ag != null)
			// ag.add(from, vid, val);
			q.putFloat(from, vid, val);
		}
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
						multiSendCounter.release();
					}
				}
			});
		}
	}

	public void queueBroadcastFloatArrayVertexData(String msg, long from,
			float[] val) throws Exception {
		SegmentedMessageQueue q = getQueue(msg);
		LongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			// if (ag != null)
			// ag.add(from, vid, val);
			q.putFloatArray(from, vid, val);
		}
	}

	public void queueFloatArrayVertexData(String msg, long from, long to,
			float[] data) throws Exception {
		getQueue(msg).putFloatArray(from, to, data);
	}
}
