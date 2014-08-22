package edu.jlime.pregel.coordinator;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;

public class CoordinatorImpl implements Coordinator {

	private HashMap<UUID, CoordinatorTask> tasks;
	private RPCDispatcher rpc;
	private HashMap<Worker, UUID> workersMap;
	private List<Worker> workersList;

	public CoordinatorImpl(List<Worker> workers) throws Exception {
		for (Worker worker : workers) {
			this.workersMap.put(worker, worker.getID());
		}
		this.workersList = workers;
	}

	public Worker getWorker(Vertex v) {
		return workersList.get(v.getId() % workersList.size());

	}

	public void start() throws Exception {
		rpc.start();
	}

	@Override
	public void finished(UUID taskID, UUID workerID) throws Exception {
		tasks.get(taskID).finished(workerID);
	}

	@Override
	public PregelGraph execute(PregelGraph input, HashMap<Vertex, byte[]> data,
			VertexFunction func, int superSteps) throws Exception {

		CoordinatorTask task = new CoordinatorTask(this);
		tasks.put(task.taskID, task);
		return task.execute(input, data, func, superSteps);

	}

	public List<Worker> getWorkers() {
		return workersList;
	}
}
