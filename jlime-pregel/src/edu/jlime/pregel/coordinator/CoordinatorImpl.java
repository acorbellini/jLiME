package edu.jlime.pregel.coordinator;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.VertexData;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;

public class CoordinatorImpl implements Coordinator {

	private HashMap<UUID, CoordinatorTask> tasks = new HashMap<>();
	private RPCDispatcher rpc;
	private ClientManager<Worker, WorkerBroadcast> workersList;

	public CoordinatorImpl(ClientManager<Worker, WorkerBroadcast> workers)
			throws Exception {
		this.workersList = workers;
	}

	public Worker getWorker(Vertex v) {
		List<Worker> all = workersList.getAll();
		return all.get(v.getId() % all.size());

	}

	public void start() throws Exception {
		rpc.start();
	}

	@Override
	public void finished(UUID taskID, UUID workerID, Boolean didWork) throws Exception {
		tasks.get(taskID).finished(workerID, didWork);
	}

	@Override
	public PregelGraph execute(PregelGraph input, VertexFunction func,
			Vertex[] vertex, Integer superSteps) throws Exception {
		CoordinatorTask task = new CoordinatorTask(this);
		tasks.put(task.taskID, task);
		return task.execute(input, vertex, func, superSteps);

	}

	public List<Worker> getWorkers() {
		return workersList.getAll();
	}
	
	public WorkerBroadcast getWorkerBroadcast() {
		return workersList.broadcast();
	}
}
