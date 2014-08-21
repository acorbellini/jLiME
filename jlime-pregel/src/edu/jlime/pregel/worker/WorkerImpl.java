package edu.jlime.pregel.worker;

import java.util.HashMap;

import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;

public class WorkerImpl implements Worker {
	HashMap<Integer, VertexFunction> vertexMap = new HashMap<>();
	private Coordinator coord;

	public WorkerImpl(Coordinator coord) {
		this.coord = coord;
	}

	@Override
	public void sendDataToVertex(Vertex vertexid, byte[] data) throws Exception {

	}

	@Override
	public void nextSuperstep(int superstep) throws Exception {

	}

	@Override
	public void schedule(Vertex vertexid, VertexFunction vertex)
			throws Exception {
		
	}

	@Override
	public void setGraph(PregelGraph input) throws Exception {
		// TODO Auto-generated method stub

	}

}
