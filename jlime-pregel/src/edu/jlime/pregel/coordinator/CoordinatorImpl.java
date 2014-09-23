package edu.jlime.pregel.coordinator;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;

public class CoordinatorImpl implements Coordinator {

	private HashMap<UUID, CoordinatorTask> tasks = new HashMap<>();

	private RPCDispatcher rpc;

	public CoordinatorImpl(RPCDispatcher rpc,
			ClientManager<Worker, WorkerBroadcast> workers) throws Exception {
		this.rpc = rpc;
	}

	public void start() throws Exception {
		rpc.start();

	}

	@Override
	public void finished(UUID taskID, UUID workerID, Boolean didWork)
			throws Exception {
		tasks.get(taskID).finished(workerID, didWork);
	}

	@Override
	public PregelExecution execute(VertexFunction func, PregelConfig config,
			Peer client) throws Exception {
		CoordinatorTask task = new CoordinatorTask(rpc,
				config.getAggregators(), client);
		tasks.put(task.taskID, task);
		return task.execute(func, config);

	}

	@Override
	public Double getAggregatedValue(UUID taskID, Long v, String k)
			throws Exception {
		return tasks.get(taskID).getAggregatedValue(v, k);

	}

	@Override
	public void setAggregatedValue(UUID taskID, Long v, String name, Double val)
			throws Exception {
		tasks.get(taskID).setAggregatedValue(v, name, val);

	}

}
