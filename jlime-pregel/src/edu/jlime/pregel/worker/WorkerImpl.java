package edu.jlime.pregel.worker;

import java.util.HashMap;

public class WorkerImpl implements Worker {
	HashMap<Integer, VertexFunction> vertexMap = new HashMap<>();

	@Override
	public void sendDataToVertex(int vertexid, byte[] data) throws Exception {

	}

	@Override
	public void nextSuperstep(int superstep) throws Exception {

	}

	@Override
	public void schedule(int vertexid, VertexFunction vertex) throws Exception {

	}

}
