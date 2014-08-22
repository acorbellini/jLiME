package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;

public class WorkerTask {

	public static class StepData {
		public HashMap<Vertex, byte[]> adyacents = new HashMap<>();
	}

	ArrayList<Vertex> modified = new ArrayList<>();

	ExecutorService exec = Executors.newFixedThreadPool(10);

	private PregelGraph graph;

	private UUID id;

	private Coordinator coord;

	private HashMap<Vertex, StepData> queue = new HashMap<>();

	private VertexFunction f;

	private Worker worker;

	public WorkerTask(PregelGraph input, Worker w, Coordinator coord, VertexFunction f,
			UUID taskID, HashMap<Vertex, byte[]> init) {
		this.graph = input;
		this.worker = w;
		this.coord = coord;
		this.id = taskID;
		this.f = f;
		for (Entry<Vertex, byte[]> e : init.entrySet()) {
			this.graph.setVal(e.getKey(), e.getValue());
		}
	}

	public void queueVertexData(Vertex from, Vertex to, byte[] data) {
		StepData vertexData = queue.get(to);
		if (vertexData == null) {
			synchronized (this) {
				vertexData = queue.get(to);
				if (vertexData == null) {
					vertexData = new StepData();
					queue.put(to, vertexData);
				}
			}
		}
		vertexData.adyacents.put(from, data);
	}

	public void nextStep(int superstep) {
		HashMap<Vertex, StepData> current = new HashMap<>(this.queue);
		this.queue = new HashMap<>();
		for (Entry<Vertex, StepData> vertex : current.entrySet()) {
			modified.add(vertex.getKey());
			exec.execute(new Runnable() {
				@Override
				public void run() {
					f.execute(vertex.getKey(), vertex.getValue(), graph);
				}
			});
		}
	}

	public boolean hasWork() {
		return queue.isEmpty();
	}

	public PregelGraph getResultGraph() {
		PregelGraph ret = new PregelGraph();
		for (Vertex vertex : modified) {
			List<Vertex> adyacency = graph.getAdyacency(vertex);
			if (adyacency != null)
				for (Vertex edge : adyacency) {
					ret.putLink(vertex, edge);
				}

		}
		return ret;
	}
}
