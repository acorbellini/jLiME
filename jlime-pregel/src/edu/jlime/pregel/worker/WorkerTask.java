package edu.jlime.pregel.worker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.MessageMerger;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.queues.MessageQueueFactory;
import edu.jlime.pregel.queues.PregelMessageQueue;
import edu.jlime.pregel.queues.SegmentedMessageQueue;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TLongArrayList;

public class WorkerTask {

	Worker[] workers;

	// volatile Future<?>[] fut;

	private SegmentedMessageQueue cache;

	private PregelMessageQueue cacheBroadcast;

	private Graph graph;

	private int taskid;

	private VertexFunction f;

	private WorkerImpl worker;

	protected Logger log = Logger.getLogger(WorkerTask.class);

	private int currentStep;

	private SegmentedMessageQueue queue;

	private PregelConfig config;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	private SplitFunction split;

	private ExecutorService vertexPool;

	private Semaphore execCount;

	private ExecutorService groupedSendPool;

	private Semaphore maxGroupedSend;

	private AtomicInteger sendCount = new AtomicInteger(0);

	private TLongArrayList vList;
	//
	private TLongArrayList currentSplit = new TLongArrayList();

	private AtomicInteger vertexCounter = new AtomicInteger(0);

	public WorkerTask(WorkerImpl w, RPCDispatcher rpc, Peer client,
			final VertexFunction func, long[] vList, final int taskID2,
			PregelConfig config) throws Exception {
		log.info("Creating task with ID " + taskID2);

		this.vertexPool = Executors.newFixedThreadPool(config.getThreads(),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Vertex Pool for Task " + func.toString()
								+ ", id:" + taskID2);
						return t;
					}
				});
		this.execCount = new Semaphore(config.getThreads() * 2);

		this.groupedSendPool = Executors.newFixedThreadPool(
				config.getThreads(), new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Grouped Send Pool for Task "
								+ func.toString() + ", id:" + taskID2);
						return t;
					}
				});
		this.maxGroupedSend = new Semaphore(config.getThreads());

		this.graph = config.getGraph().getGraph(rpc);

		this.worker = w;
		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc,
				CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				client);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new WorkerFilter(), client);
		this.taskid = taskID2;

		if (config.isExecuteOnAll()) {
			long start = System.currentTimeMillis();
			log.info("Creating vertex list for the whole graph.");
			TLongArrayList gatherVList = new TLongArrayList();
			for (Long vid : graph.vertices()) {
				gatherVList.add(vid);
			}
			this.vList = gatherVList;
			log.info("Finished creating vertex list for the whole graph in "
					+ (System.currentTimeMillis() - start) / 1000f + " sec.");
		} else
			this.vList = new TLongArrayList(vList);

		this.setConfig(config);
		MessageMerger merger = config.getMerger();
		MessageQueueFactory fact = merger != null ? merger.getFactory()
				: MessageQueueFactory.simple(null);

		this.queue = new SegmentedMessageQueue(this, config.getSegments(),
				Integer.MAX_VALUE, fact, config.getThreads());
		this.cache = new SegmentedMessageQueue(this, config.getSegments(),
				config.getQueueLimit(), fact, config.getThreads());
		this.cacheBroadcast = fact.getMQ();
		this.f = func;
	}

	public void queueVertexData(long from, long to, Object val) {
		this.queue.put(from, to, val);
	}

	public void queueFloatVertexData(long from, long to, float val) {
		this.queue.putFloat(from, to, val);
	}

	public void queueBroadcastVertexData(long from, Object val) {
		TLongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			queue.put(from, vid, val);
		}
	}

	public void queueBroadcastFloatVertexData(long from, float val) {
		TLongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			queue.putFloat(from, vid, val);
		}
	}

	public void nextStep(int superstep, SplitFunction func) throws Exception {
		log.info("Executing step " + superstep + " on Worker " + worker.getID());

		this.split = func;

		Peer[] peers = this.split.getPeers();

		workers = new Worker[peers.length];
		for (int i = 0; i < peers.length; i++) {
			workers[i] = workerMgr.get(peers[i]);
		}

		this.currentStep = superstep;

		queue.switchQueue();

		// It's done twice, called with -1 and 0.
		log.info("Loading local slice of vertices from " + vList.size()
				+ " vertices.");

		currentSplit.clear();
		Peer localPeer = workerMgr.getLocalPeer();
		List<Peer> peers2 = workerMgr.getPeers();

		TLongIterator it = vList.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			if (split.getPeer(vid, peers2).equals(localPeer))
				currentSplit.add(vid);
		}
		log.info("Loaded slice " + currentSplit.size() + " vertices.");
	}

	public void execute() throws Exception {
		int size = queue.readOnlySize();

		if (size == 0) {
			// if (log.isDebugEnabled())
			log.info("Queue was empty, finished step " + currentStep
					+ " on Worker " + worker.getID());
			try {
				coordMgr.getFirst().finished(getTaskid(), this.worker.getID(),
						false);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return;
		}

		log.info("Starting Execution on worker " + worker.getID() + " for "
				+ size + " messages.");

		// log.info(queue.readOnlySize());
		int count = 0;
		Iterator<List<PregelMessage>> it = queue.iterator();
		while (it.hasNext()) {
			List<PregelMessage> list = it.next();
			long vid = list.get(0).getTo();
			count++;
			printCompleted(size, count);
			execVertex(vid, list);
		}

		synchronized (vertexCounter) {
			while (vertexCounter.get() != 0)
				try {
					vertexCounter.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}

		synchronized (sendCount) {
			while (sendCount.get() != 0)
				try {
					sendCount.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}

		log.info("Finished work for step " + currentStep + " on Worker "
				+ worker.getID());

		cache.switchQueue();
		cache.flush(this);

		cacheBroadcast.switchQueue();
		cacheBroadcast.flush(this);

		coordMgr.getFirst().finished(getTaskid(), this.worker.getID(), true);
	}

	private void printCompleted(int total, double currentCount)
			throws Exception {
		int fraction = ((int) (total / (double) 10));
		if ((currentCount % fraction) == 0) {
			double completed = ((currentCount / total) * 100);
			// if (log.isDebugEnabled())
			log.info("Completed work on worker " + worker.getID() + ": "
					+ Math.ceil(completed) + " % ");
		}
	}

	private void execVertex(final Long currentVertex,
			final List<PregelMessage> messages) throws InterruptedException {
		execCount.acquire();
		vertexCounter.incrementAndGet();
		vertexPool.execute(new Runnable() {
			@Override
			public void run() {

				try {
					if (log.isDebugEnabled())
						log.debug("Executing function on vertex "
								+ currentVertex);
					WorkerContext ctx = new WorkerContext(WorkerTask.this,
							currentVertex);
					f.execute(currentVertex, messages, ctx);
					if (log.isDebugEnabled())
						log.debug("Finished executing function on vertex "
								+ currentVertex);
					// ctx.finished();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					execCount.release();
				}

				vertexCounter.decrementAndGet();
				synchronized (vertexCounter) {
					vertexCounter.notify();
				}
			}
		});
	}

	public Graph getGraph() {
		return graph;
	}

	public void sendAll(long from, Object msg) throws Exception {
		synchronized (cacheBroadcast) {
			checkBroadCacheSize();
			cacheBroadcast.put(from, -1l, msg);
		}

	}

	private void checkBroadCacheSize() throws Exception {
		if (cacheBroadcast.currentSize() == config.getBroadcastQueue()) {
			cacheBroadcast.switchQueue();
			cacheBroadcast.flush(this);
		}
	}

	public void send(long from, long to, Object val) throws Exception {
		cache.put(from, to, val);
	}

	public Worker getWorker(long vid) {
		return workers[split.hash(vid)];
	}

	public Integer getSuperStep() {
		return currentStep;
	}

	public Double getAggregatedValue(Long v, String k) throws Exception {
		return coordMgr.getFirst().getAggregatedValue(getTaskid(), v, k);
	}

	public void setAggregatedValue(Long v, String string, double currentVal)
			throws Exception {
		coordMgr.getFirst().setAggregatedValue(getTaskid(), v, string,
				currentVal);
	}

	public PregelConfig getConfig() {
		return config;
	}

	public void setConfig(PregelConfig config) {
		this.config = config;
	}

	public void cleanup() {
		this.queue.clean();
		this.cache.clean();
		vertexPool.shutdown();
		groupedSendPool.shutdown();
	}

	public void sendAllFloat(long from, float val) throws Exception {
		synchronized (cacheBroadcast) {
			checkBroadCacheSize();
			cacheBroadcast.putFloat(from, -1l, val);
		}
	}

	public void sendFloat(long from, long to, float val) throws Exception {
		cache.putFloat(from, to, val);
	}

	public void outputObject(long from, long to, Object val) throws Exception {
		if (to != -1l)
			getWorker(to).sendMessage(from, to, val, getTaskid());
		else
			workerMgr.broadcast().sendBroadcastMessage(from, val, getTaskid());
	}

	public void outputFloat(long from, long to, float value) throws Exception {
		if (to != -1l)
			getWorker(to).sendFloatMessage(from, to, value, getTaskid());
		else
			workerMgr.broadcast().sendFloatBroadcastMessage(from, value,
					getTaskid());
	}

	public int getTaskid() {
		return taskid;
	}

	public void sendAllDouble(long from, double val) throws Exception {
		synchronized (cacheBroadcast) {
			checkBroadCacheSize();
			cacheBroadcast.putDouble(from, -1l, val);
		}
	}

	public void sendDouble(long from, long to, double val) {
		cache.putDouble(from, to, val);
	}

	public void sendDoubles(HashMap<Worker, TLongArrayList> keys,
			final HashMap<Worker, TDoubleArrayList> values)
			throws InterruptedException {
		final Semaphore count = new Semaphore(-keys.size() + 1);

		for (final Entry<Worker, TLongArrayList> e : keys.entrySet()) {
			sendCount.incrementAndGet();
			maxGroupedSend.acquire();
			groupedSendPool.execute(new Runnable() {

				@Override
				public void run() {
					Worker w = e.getKey();

					TLongArrayList toList = e.getValue();
					TDoubleArrayList valList = values.get(w);
					try {
						w.sendDoubleMessage(-1l, toList.toArray(),
								valList.toArray(), taskid);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					count.release();
					maxGroupedSend.release();
					sendCount.decrementAndGet();
					synchronized (sendCount) {
						sendCount.notify();
					}
				}
			});

		}
		count.acquire();
	}

	public void sendFloats(HashMap<Worker, TLongArrayList> keys,
			final HashMap<Worker, TFloatArrayList> values) throws Exception {
		final Semaphore count = new Semaphore(-keys.size() + 1);

		for (final Entry<Worker, TLongArrayList> e : keys.entrySet()) {
			sendCount.incrementAndGet();
			maxGroupedSend.acquire();
			groupedSendPool.execute(new Runnable() {

				@Override
				public void run() {
					Worker w = e.getKey();

					TLongArrayList toList = e.getValue();
					TFloatArrayList valList = values.get(w);
					try {
						w.sendFloatMessage(-1l, toList.toArray(),
								valList.toArray(), taskid);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					count.release();
					maxGroupedSend.release();
					sendCount.decrementAndGet();
					synchronized (sendCount) {
						sendCount.notify();
					}
				}
			});

		}
		count.acquire();
	}

	public void queueDoubleVertexData(long from, long to, double val) {
		this.queue.putDouble(from, to, val);
	}

	public void outputDouble(long from, long to, double val) throws Exception {
		if (to != -1l)
			getWorker(to).sendDoubleMessage(from, to, val, getTaskid());
		else
			workerMgr.broadcast().sendDoubleBroadcastMessage(from, val,
					getTaskid());
	}

	public void queueBroadcastDoubleVertexData(long from, double val) {
		TLongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			queue.putDouble(from, vid, val);
		}
	}
}
