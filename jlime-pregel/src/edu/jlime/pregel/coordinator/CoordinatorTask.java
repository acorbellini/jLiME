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
import edu.jlime.pregel.worker.PregelMessage;
import edu.jlime.pregel.worker.WorkerFilter;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;

public class CoordinatorTask {
	UUID taskID = UUID.randomUUID();

	private volatile boolean finished = true;

	private Set<UUID> currentStep = new HashSet<>();

	private Logger log = Logger.getLogger(CoordinatorTask.class);

	private HashMap<String, Aggregator> aggregators;

	private Peer cli;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	public CoordinatorTask(RPCDispatcher rpc, HashMap<String, Aggregator> aggs,
			Peer cli) {
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

	public PregelExecution execute(VertexFunction func, PregelConfig config)
			throws Exception {
		SplitFunction split = config.getSplit();

		split.update();

		// Init workers
		for (Worker w : workerMgr.getAll()) {
			currentStep.add(w.getID());
			w.createTask(taskID, cli, func, config);
		}
		Iterable<Long> vertex = config.getvList();
		if (config.isExecuteOnAll()) {
			vertex = config.getGraph().vertices();

		}
		for (Long vid : vertex) {
			Worker worker = workerMgr.get(split.getPeer(vid,
					workerMgr.getPeers()));
			worker.sendMessage(new PregelMessage(-1, vid, null), taskID);
		}

		int step = 0;
		for (; step < config.getMaxSteps(); step++) {
			log.info("Step: " + step);
			finished = true;

			if (log.isDebugEnabled())
				log.debug("Running superstep " + step + " Remaining "
						+ currentStep.size());

			split.update();

			workerMgr.broadcast().nextSuperstep(step, taskID, split);

			workerMgr.broadcast().execute(taskID);

			synchronized (this) {
				while (!currentStep.isEmpty())
					wait();
			}

			if (log.isDebugEnabled())
				log.debug("Finished superstep " + step);

			if (finished)
				break;

			for (Worker w : workerMgr.getAll())
				currentStep.add(w.getID());

			for (Entry<String, Aggregator> e : aggregators.entrySet()) {
				e.getValue().superstep(step);
			}

		}
		log.info("Finished");
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