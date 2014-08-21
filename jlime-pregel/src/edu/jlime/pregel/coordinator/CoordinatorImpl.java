package edu.jlime.pregel.coordinator;

import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;

public class CoordinatorImpl implements Coordinator {

	private RPCDispatcher rpc;
	private List<Worker> workers;

	public CoordinatorImpl(List<Worker> workers) {
		this.workers = workers;
	}

	private Worker getWorker(Vertex v) {
		return workers.get(v.getId() % workers.size());

	}

	public void start() throws Exception {
		rpc.start();
	}

	@Override
	public void finished(UUID workerID) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public PregelGraph execute(List<Vertex> vertex, VertexFunction func)
			throws Exception {
		for (Vertex vertexID : vertex) {
			getWorker(vertexID).schedule(vertexID, func);
		}
		return null;
	}

	@Override
	public void setGraph(PregelGraph input) throws Exception {
		// TODO Auto-generated method stub

	}
}
