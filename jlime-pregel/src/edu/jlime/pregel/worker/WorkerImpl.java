package edu.jlime.pregel.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.rpc.ClientManager;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;

public class WorkerImpl implements Worker {

	HashMap<UUID, WorkerTask> contexts = new HashMap<>();

	private ClientManager<Coordinator, CoordinatorBroadcast> coordCli;

	private UUID id = UUID.randomUUID();

	private ClientManager<Worker, WorkerBroadcast> workerCli;

	public WorkerImpl(ClientManager<Coordinator, CoordinatorBroadcast> coord,
			ClientManager<Worker, WorkerBroadcast> workers) {
		this.coordCli = coord;
		this.workerCli = workers;
	}

	@Override
	public void sendDataToVertex(Vertex from, Vertex to, VertexData data,
			UUID taskID) throws Exception {
		contexts.get(taskID).queueVertexData(from, to, data);
	}

	@Override
	public UUID getID() throws Exception {
		return id;
	}

	@Override
	public void nextSuperstep(Integer superstep, UUID taskID) throws Exception {
		contexts.get(taskID).nextStep(superstep);
	}

	@Override
	public void createTask(PregelGraph input, VertexFunction func, UUID taskID,
			HashSet<Vertex> init) throws Exception {
		contexts.put(taskID, new WorkerTask(input, this, coordCli.first(),
				func, taskID, init));
	}

	@Override
	public PregelGraph getResult(UUID taskID) throws Exception {
		return contexts.get(taskID).getResultGraph();
	}

	public Worker getWorker(Vertex v) {
		List<Worker> workers = workerCli.getAll();
		return workers.get(v.getId() % workers.size());

	}

}
