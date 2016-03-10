package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Client;
import edu.jlime.core.rpc.RPC;
import edu.jlime.pregel.PregelSubgraph;
import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.client.ContextResult;
import edu.jlime.pregel.client.CoordinatorFilter;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.mergers.MessageMerger;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.queues.FloatMessageQueue;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class WorkerTask {
	protected Logger log = Logger.getLogger(WorkerTask.class);

	Map<String, Aggregator> aggregators = new HashMap<>();

	private Worker[] workers;

	private PregelGraph graph;

	private int taskid;

	private VertexFunction<PregelMessage> f;

	private WorkerImpl worker;

	private int currentStep;

	private PregelConfig config;

	InputQueue queue;

	private Client<Coordinator, CoordinatorBroadcast> coordMgr;

	private Client<Worker, WorkerBroadcast> workerMgr;

	private SplitFunction split;

	private ExecutorService vertexPool;

	private VertexList graphVertexList;

	private Map<String, PregelSubgraph> subgraphs = new ConcurrentHashMap<>();

	private boolean hasResult = false;

	public WorkerTask(WorkerImpl w, RPC rpc, Peer client,
			final VertexFunction<PregelMessage> func, long[] vList,
			final int taskID, PregelConfig config) throws Exception {
		this.log.info("Creating task with ID " + taskID);
		this.graph = config.getGraph().getGraph(rpc);
		for (Entry<String, Aggregator> e : config.getAggregators().entrySet()) {
			aggregators.put(e.getKey(), e.getValue().copy());
		}

		for (Entry<String, TLongHashSet> e : config.getSubgraphs().entrySet()) {
			subgraphs.put(e.getKey(), new PregelSubgraph(e.getValue(), graph));
		}

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

		this.worker = w;

		this.coordMgr = rpc.manage(
				new CoordinatorFactory(rpc, CoordinatorServer.COORDINATOR_KEY),
				new CoordinatorFilter(), client);
		this.workerMgr = rpc.manage(
				new WorkerFactory(rpc, WorkerServer.WORKER_KEY),
				new WorkerFilter(), client);

		this.taskid = taskID;

		this.config = config;

		this.queue = new InputQueue(config);

		this.f = func;
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

		queue.switchQueue();
	}

	public void execute() throws Exception {
		long init = System.currentTimeMillis();
		try {
			final TLongArrayList vList = new TLongArrayList();

			final TLongArrayList remote = new TLongArrayList();

			if (queue.broadcastSize() > 0) { // activate all vertices.
				if (this.graphVertexList == null) {
					this.graphVertexList = this.config.isPersitentVertexList()
							? new PersistedVertexList() : new InMemVertexList();
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
				if (queue.broadcastSubgraphSize() > 0) {
					Peer localPeer = this.workerMgr.getLocalPeer();
					List<Peer> peers2 = this.workerMgr.getPeers();
					for (Entry<String, TLongHashSet> e : config.getSubgraphs()
							.entrySet()) {
						TLongIterator it = e.getValue().iterator();
						while (it.hasNext()) {
							long vid = it.next();
							if (this.split.getPeer(vid, peers2)
									.equals(localPeer))
								activeVertices.add(vid);
						}
					}
				}

				activeVertices.addAll(queue.getKeys());

				TLongIterator it = activeVertices.iterator();
				while (it.hasNext()) {
					long v = it.next();
					if (!graph.isLocal(v))
						remote.add(v);
					else
						vList.add(v);
				}
			}

			final int size = vList.size() + remote.size();

			if (size == 0) {
				this.log.info("Queue was empty, finished step " + currentStep
						+ " on Worker " + this.worker.getID());
				this.coordMgr.getFirst().finished(getTaskid(),
						this.worker.getID(), false, aggregators);
			} else {
				this.log.info("Starting Execution on worker " + worker.getID()
						+ " for " + size + " messages.");
				List<Future<ContextResult>> toSend = new ArrayList<>();

				final int threads = this.config.getThreads();
				final AtomicInteger count = new AtomicInteger(0);
				// Remote vertices.
				{
					if (remote.size() > 0) {
						graph.preload(remote);

						for (int i = 0; i < this.config.getThreads(); i++) {
							final int currentIndex = i;
							toSend.add(this.vertexPool
									.submit(executeVertexRange(size, threads,
											count, currentIndex, remote)));
						}
					}
				}

				{

					for (int i = 0; i < this.config.getThreads(); i++) {
						final int currentIndex = i;
						toSend.add(this.vertexPool.submit(executeVertexRange(
								size, threads, count, currentIndex, vList)));
					}
				}

				ContextResult finalResult = null;
				while (!toSend.isEmpty()) {
					synchronized (this) {
						if (!hasResult)
							wait(5000);
						hasResult = false;
					}
					Iterator<Future<ContextResult>> it = toSend.iterator();
					while (it.hasNext()) {
						Future<ContextResult> future = it.next();
						if (future.isDone()) {
							ContextResult ctx = future.get();
							if (finalResult == null)
								finalResult = ctx;
							else
								finalResult.mergeWith(ctx, config);

							it.remove();
						}
					}
				}

				for (Entry<String, Aggregator> e : finalResult.getAggs()
						.entrySet()) {
					this.aggregators.get(e.getKey()).merge(e.getValue());
				}

				send(finalResult);

				if (remote.size() > 0)
					graph.flush(remote);

				log.info("Finished work for step " + currentStep + " on Worker "
						+ worker.getID());
				coordMgr.getFirst().finished(getTaskid(), this.worker.getID(),
						true, aggregators);
			}
		} catch (Throwable e) {
			e.printStackTrace();
			coordMgr.getFirst()
					.error(new Exception(
							"Worker Task exception ID " + getTaskid(), e),
					getTaskid(), this.worker.getID());
		}

		log.info("Completed step in " + (System.currentTimeMillis() - init));
	}

	private void send(ContextResult finalResult) throws Exception {
		WorkerBroadcast broadcast = workerMgr.broadcast();
		{
			TObjectFloatIterator<Pair<String, String>> it = finalResult
					.getSg_broadcast().iterator();
			while (it.hasNext()) {
				it.advance();
				broadcast.sendBroadcastMessageSubgraphFloat(it.key().left,
						it.key().right, -1l, it.value(), taskid);
			}
		}

		{
			TObjectFloatIterator<String> it_br = finalResult.getBroadcast()
					.iterator();
			while (it_br.hasNext()) {
				it_br.advance();
				broadcast.sendFloatBroadcastMessage(it_br.key(), -1l,
						it_br.value(), taskid);
			}
		}
		List<Future<Void>> futures = new ArrayList<>();
		HashMap<String, HashMap<Worker, TLongFloatHashMap>> data = new HashMap<>();
		for (HashMap<String, TLongFloatHashMap> e : finalResult.getResult()) {
			for (Entry<String, TLongFloatHashMap> e2 : e.entrySet()) {
				String type = e2.getKey();
				MessageMerger merger = config.getMerger(type);
				HashMap<Worker, TLongFloatHashMap> map = data.get(type);
				if (map == null) {
					map = new HashMap<>();
					data.put(type, map);
				}
				TLongFloatIterator it = e2.getValue().iterator();
				while (it.hasNext()) {
					it.advance();
					Worker w = getWorker(it.key());
					TLongFloatHashMap submap = map.get(w);
					if (submap == null) {
						submap = new TLongFloatHashMap();
						map.put(w, submap);
					}
					merger.merge(it.key(), it.value(), submap);
				}
			}
		}
		ExecutorService service = Executors
				.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (Entry<String, HashMap<Worker, TLongFloatHashMap>> e : data
				.entrySet()) {
			final String type = e.getKey();
			for (final Entry<Worker, TLongFloatHashMap> e2 : e.getValue()
					.entrySet())
				futures.add(service.submit(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						e2.getKey().sendFloatMessage(type, -1,
								e2.getValue().keys(), e2.getValue().values(),
								taskid);
						return null;
					}
				}));
		}

		service.shutdown();

		for (Future<Void> future : futures) {
			future.get();
		}

	}

	private Callable<ContextResult> executeVertexRange(final int size,
			final int threads, final AtomicInteger count, final int threadID,
			final TLongArrayList vList) throws Exception {
		return new Callable<ContextResult>() {
			@Override
			public ContextResult call() throws Exception {
				Context ctx = new Context(WorkerTask.this);
				int range = vList.size() / threads;
				int from = threadID * range;
				int to = threadID == threads - 1 ? vList.size() : from + range;
				for (int i = from; i < to; i++) {
					long currentVertex = vList.get(i);
					Iterator<PregelMessage> messages = queue
							.getMessages(currentVertex);
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
				synchronized (WorkerTask.this) {
					hasResult = true;
					WorkerTask.this.notify();
				}
				return ctx.getResult();
			}
		};

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

	public PregelGraph getGraph() {
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
	}

	public int getTaskid() {
		return taskid;
	}

	public void queueFloatVertexData(String msg, long from, long to, float val)
			throws Exception {
		((FloatMessageQueue) queue.getQueue(msg)).putFloat(from, to, val);

	}

	public void queueBroadcastFloatVertexData(String msg, long from, float val)
			throws Exception {
		FloatMessageQueue q = (FloatMessageQueue) queue.getBroadcastQueue(msg);
		q.putFloat(from, -1l, val);
	}

	public void queueFloatVertexData(String msgType, long from, long[] to,
			float[] vals) {
		FloatMessageQueue q = (FloatMessageQueue) queue.getQueue(msgType);
		q.putFloat(from, to, vals);
	}

	public PregelSubgraph getSubgraph(String string) {
		return subgraphs.get(string);
	}

	public void queueBroadcastSubgraphFloat(String msgType, String subgraph,
			float val) {
		FloatMessageQueue q = (FloatMessageQueue) queue
				.getBroadcastSubgraphQueue(msgType, subgraph);
		q.putFloat(-1, -1, val);
	}
}
