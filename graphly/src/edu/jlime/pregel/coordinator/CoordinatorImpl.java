package edu.jlime.pregel.coordinator;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Client;
import edu.jlime.core.rpc.RPC;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;

public class CoordinatorImpl implements Coordinator {

	// AtomicInteger taskCount = new AtomicInteger(0);

	private ArrayList<CoordinatorTask> tasks = new ArrayList<>();

	private RPC rpc;

	public CoordinatorImpl(RPC rpc, Client<Worker, WorkerBroadcast> workers)
			throws Exception {
		this.rpc = rpc;
	}

	public void start() throws Exception {
		rpc.start();
	}

	@Override
	public void finished(int taskID, UUID workerID, Boolean didWork,
			Map<String, Aggregator> ags) throws Exception {
		tasks.get(taskID).finished(workerID, didWork, ags);
	}

	@Override
	public PregelExecution execute(VertexFunction func, long[] vList,
			PregelConfig config, Peer client) throws Exception {
		// int taskID = taskCount.getAndIncrement();

		int taskID = 0;
		CoordinatorTask task = null;
		synchronized (tasks) {
			taskID = tasks.size();
			task = new CoordinatorTask(taskID, rpc, config.getAggregators(),
					client);
			tasks.add(task);

		}
		PregelExecution ret;
		try {
			ret = task.execute(func, vList, config);
		} catch (Exception e) {
			throw new Exception("Coordinator Task Exception", e);
		}

		tasks.set(taskID, null);

		return ret;

	}

	@Override
	public void error(Exception e, int taskid, UUID id) throws Exception {
		tasks.get(taskid).error(id, e);
	}

	// @Override
	// public Double getAggregatedValue(int taskID, Long v, String k)
	// throws Exception {
	// return tasks.get(taskID).getAggregatedValue(v, k);
	//
	// }
	//
	// @Override
	// public void setAggregatedValue(int taskID, Long v, String name, Double
	// val)
	// throws Exception {
	// tasks.get(taskID).setAggregatedValue(v, name, val);
	// }

}
