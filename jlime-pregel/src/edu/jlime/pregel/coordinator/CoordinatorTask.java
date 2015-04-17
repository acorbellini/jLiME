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
import edu.jlime.pregel.worker.GenericPregelMessage;
import edu.jlime.pregel.worker.PregelMessage;
import edu.jlime.pregel.worker.WorkerFilter;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;

public class CoordinatorTask {
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

	public synchronized void finished(UUID workerID, Boolean didWork) {
		if (didWork)
			finished = false;

		if (log.isDebugEnabled())
			log.debug("Received finished  from worker : " + workerID);

		currentStep.remove(workerID);

		notifyAll();
		if (log.isDebugEnabled())
			log.debug("Remaining in step: " + currentStep.size());
	}

	public PregelExecution execute(final VertexFunction func, long[] vList,
			final PregelConfig config) throws Exception {
		final SplitFunction split = config.getSplit();

		split.update(workerMgr.getPeers());
		long[] list = vList;

		log.info("Creating tasks");
		workerMgr.broadcast().createTask(taskID, cli, func, list, config);

		log.info("Initial Superstep");
		workerMgr.broadcast().nextSuperstep(-1, taskID, split);

		log.info("Broadcasting initial message");
		workerMgr.broadcast().sendBroadcastMessage(-1l, null, taskID);

		log.info("Setup of Superstep 0");
		workerMgr.broadcast().nextSuperstep(0, taskID, split);

		int step = 0;
		for (; step < config.getMaxSteps(); step++) {
			finished = true;

			// if (log.isDebugEnabled())
			log.info("Running superstep " + step + " Remaining "
					+ (config.getMaxSteps() - step));

			workerMgr.broadcast().execute(taskID);

			synchronized (this) {
				while (!currentStep.isEmpty())
					wait();
			}

			// if (log.isDebugEnabled())
			log.info("Finished superstep " + step);

			if (finished)
				break;

			for (Worker w : workerMgr.getAll())
				currentStep.add(w.getID());

			for (Entry<String, Aggregator> e : aggregators.entrySet()) {
				e.getValue().superstep(step);
			}

			split.update(workerMgr.getPeers());

			workerMgr.broadcast().nextSuperstep(step + 1, taskID, split);

		}
		log.info("Finished");

		workerMgr.broadcast().cleanup(taskID);

		return new PregelExecution(step, taskID);
	}

	// private Graph mergeGraph() throws Exception {
	// if (log.isDebugEnabled())
	// log.debug("Merging Graph");
	//
	// Graph ret = null;
	// for (Worker w : coord.getWorkers()) {
	// Graph result = w.getResult(taskID);
	// if (ret == null)
	// ret = result;
	// if (ret != result)
	// ret.merge(result);
	// }
	// return ret;
	// }

	public Double getAggregatedValue(Long v, String k) {
		log.info("Obtaining aggregated value from " + k + " for " + v);
		Aggregator a = aggregators.get(k);
		if (a != null) {
			Double val = a.getVal(v);
			log.info("Obtained " + k + " for " + v + ": " + val);
			return val;
		}
		log.info("No aggregator by that name: " + k);
		return null;
	}

	public void setAggregatedValue(Long v, String name, Double val) {
		Aggregator a = aggregators.get(name);
		if (a != null)
			a.setVal(v, val);
	}
}