package edu.jlime.pregel.coordinator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.CoordinatorFilter;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.WorkerFilter;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;

public class CoordinatorTask {
	private static final String INIT_MSG = "_INIT_MSG";

	int taskID;

	private volatile boolean finished = true;

	private Set<UUID> currentStep = new HashSet<>();

	private Logger log = Logger.getLogger(CoordinatorTask.class);

	private HashMap<String, Aggregator> aggregators;

	private Peer cli;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	public CoordinatorTask(int taskId, RPCDispatcher rpc,
			HashMap<String, Aggregator> aggs, Peer cli) {
		this.taskID = taskId;
		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc,
				CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				this.cli);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new WorkerFilter(), this.cli);
		this.aggregators = aggs;
		this.cli = cli;
	}

	public synchronized void finished(UUID workerID, Boolean didWork,
			HashMap<String, Aggregator> ags) {

		for (Entry<String, Aggregator> e : ags.entrySet())
			aggregators.get(e.getKey()).merge(e.getValue());

		if (didWork)
			finished = false;

		log.info("Received finished  from worker : " + workerID);

		currentStep.remove(workerID);

		notifyAll();

		log.info("Remaining in step: " + currentStep.size());
	}

	public PregelExecution execute(final VertexFunction func, long[] list,
			final PregelConfig config) throws Exception {

		HaltCondition haltCondition = config.getHaltCondition();

		final SplitFunction split = config.getSplit();

		split.update(workerMgr.getPeers());

		log.info("Creating tasks");
		long startInit = System.currentTimeMillis();
		workerMgr.broadcast().createTask(taskID, cli, func, list, config);

		log.info("Finished creating tasks in "
				+ (System.currentTimeMillis() - startInit) / 1000f + " sec.");

		log.info("Initial Superstep");
		workerMgr.broadcast().nextSuperstep(-1, taskID, split);

		log.info("Broadcasting initial message");
		workerMgr.broadcast().sendBroadcastMessage(INIT_MSG, -1l, null, taskID);

		int step = 0;
		for (; step < config.getMaxSteps()
				&& (haltCondition == null || !haltCondition.eval(this, step)); step++) {

			log.info("Updating aggregators.");
			for (Entry<String, Aggregator> e : aggregators.entrySet()) {
				e.getValue().superstep(step);
			}

			log.info("Initializing current list of workers");
			for (Worker w : workerMgr.getAll())
				currentStep.add(w.getID());

			long start = System.currentTimeMillis();
			finished = true;

			workerMgr.broadcast().nextSuperstep(step, taskID, split);

			log.info("Running superstep " + step + " Remaining "
					+ (config.getMaxSteps() - step));

			workerMgr.broadcast().execute(taskID);

			synchronized (this) {
				while (!currentStep.isEmpty())
					wait();
			}

			log.info("Finished superstep " + step);

			if (finished)
				break;

			split.update(workerMgr.getPeers());

			log.info("Finished superstep " + step + " in "
					+ (System.currentTimeMillis() - start) / 1000f + " sec.");

		}
		log.info("Finished");

		workerMgr.broadcast().cleanup(taskID);

		return new PregelExecution(step, taskID);
	}

	public Aggregator getAggregator(String ag) {
		return aggregators.get(ag);
	}
}