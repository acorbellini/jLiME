package edu.jlime.pregel.coordinator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Client;
import edu.jlime.core.rpc.RPC;
import edu.jlime.jd.mapreduce.ForkJoinException;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.CoordinatorFilter;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.mergers.MessageMergers;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerFilter;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import gnu.trove.list.array.TLongArrayList;

public class CoordinatorTask {
	ForkJoinException excep = new ForkJoinException();

	private static final String INIT_MSG = "_INIT_MSG";

	int taskID;

	private volatile boolean finished = true;

	private HashMap<UUID, Peer> currentStep = new HashMap<>();

	private Logger log = Logger.getLogger(CoordinatorTask.class);

	private HashMap<String, Aggregator> aggregators;

	private Peer cli;

	private Client<Coordinator, CoordinatorBroadcast> coordMgr;

	private Client<Worker, WorkerBroadcast> workerMgr;

	public CoordinatorTask(int taskId, RPC rpc, HashMap<String, Aggregator> aggs, Peer cli) {
		this.taskID = taskId;
		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc, CoordinatorServer.COORDINATOR_KEY),
				new CoordinatorFilter(), this.cli);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc, WorkerServer.WORKER_KEY), new WorkerFilter(), this.cli);
		this.aggregators = aggs;
		this.cli = cli;
	}

	public synchronized void finished(UUID workerID, Boolean didWork, Map<String, Aggregator> ags) {

		for (Entry<String, Aggregator> e : ags.entrySet())
			aggregators.get(e.getKey()).merge(e.getValue());

		if (didWork)
			finished = false;

		log.info("Received finished  from worker : " + workerID);

		currentStep.remove(workerID);

		notifyAll();

		log.info("Remaining in step: " + currentStep.size());
	}

	public PregelExecution execute(final VertexFunction<PregelMessage> func, long[] list, final PregelConfig config)
			throws Exception {

		config.merger(INIT_MSG, MessageMergers.floatSum());

		long startTask = System.currentTimeMillis();

		HaltCondition haltCondition = config.getHaltCondition();

		final SplitFunction split = config.getSplit();

		split.update(workerMgr.getPeers());

		log.info("Creating tasks");
		long startInit = System.currentTimeMillis();
		workerMgr.broadcast().createTask(taskID, cli, func, list, config);

		log.info("Finished creating tasks in " + (System.currentTimeMillis() - startInit) / 1000f + " sec.");

		log.info("Initial Superstep");
		workerMgr.broadcast().nextSuperstep(-1, taskID, split, aggregators);

		log.info("Broadcasting initial message");
		if (config.isExecuteOnAll())
			workerMgr.broadcast().sendFloatBroadcastMessage(INIT_MSG, -1l, 0f, taskID);
		else {
			HashMap<Worker, TLongArrayList> toSend = new HashMap<>();

			for (long l : list) {
				Worker w = workerMgr.get(split.getPeer(l, workerMgr.getPeers()));
				TLongArrayList curr = toSend.get(w);
				if (curr == null) {
					curr = new TLongArrayList();
					toSend.put(w, curr);
				}
				curr.add(l);
			}

			for (Entry<Worker, TLongArrayList> e : toSend.entrySet()) {
				long[] to = e.getValue().toArray();
				float[] val = new float[to.length];
				Arrays.fill(val, 0f);
				e.getKey().sendFloatMessage(INIT_MSG, -1, to, val, taskID);
			}
		}

		int step = 0;
		for (; step < config.getMaxSteps() && (haltCondition == null || !haltCondition.eval(this, step)); step++) {

			log.info("Initializing current list of workers");
			for (Peer w : workerMgr.getPeers())
				currentStep.put(workerMgr.get(w).getID(), w);

			long start = System.currentTimeMillis();
			finished = true;

			workerMgr.broadcast().nextSuperstep(step, taskID, split, aggregators);

			log.info("Running superstep " + step + " Remaining " + (config.getMaxSteps() - step));

			workerMgr.broadcast().execute(taskID);
			long init = System.currentTimeMillis();
			long l = 2000000;
			synchronized (this) {
				while (!currentStep.isEmpty() && l > 0) {
					l -= System.currentTimeMillis() - init;
					wait(l);
				}
			}
			
			if (l < 0)
				throw new Exception("Timeout Exception");
			
			log.info("Finished superstep " + step);

			// workerMgr.broadcast().finishedProcessing(taskID);

			log.info("Updating aggregators.");
			for (Entry<String, Aggregator> e : aggregators.entrySet()) {
				e.getValue().superstep(step);
			}

			if (!excep.isEmpty())
				throw excep;

			if (finished)
				break;

			split.update(workerMgr.getPeers());

			log.info("Finished superstep " + step + " in " + (System.currentTimeMillis() - start) / 1000f + " sec.");
		}
		log.info("Finished in " + (System.currentTimeMillis() - startTask) / 1000 + " sec.");

		workerMgr.broadcast().cleanup(taskID);

		return new PregelExecution(step, taskID, aggregators);
	}

	public Aggregator getAggregator(String ag) {
		return aggregators.get(ag);
	}

	public synchronized void error(UUID id, Exception e) {
		if (currentStep.isEmpty())
			return;
		excep.put(currentStep.get(id), e);

		log.info("Received error  from worker : " + id);

		currentStep.clear();

		notifyAll();

		log.info("Remaining in step: " + currentStep.size());

	}
}