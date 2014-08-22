package edu.jlime.pregel.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;

public class CoordinatorTask {
	UUID taskID = UUID.randomUUID();

	private Set<UUID> currentStep = new HashSet<>();

	private CoordinatorImpl coord;

	public CoordinatorTask(CoordinatorImpl coordinatorImpl) {
		this.coord = coordinatorImpl;
	}

	public synchronized void finished(UUID workerID) {
		currentStep.remove(workerID);
		notify();
	}

	public PregelGraph execute(PregelGraph input,
			HashMap<Vertex, byte[]> initialData, VertexFunction func,
			int supersteps) throws Exception {

		// Divide vertex list.
		HashMap<Worker, List<Vertex>> divided = new HashMap<>();
		for (Vertex vertexID : initialData.keySet()) {
			Worker worker = coord.getWorker(vertexID);
			List<Vertex> sublist = divided.get(worker);
			if (sublist == null) {
				sublist = new ArrayList<>();
				divided.put(worker, sublist);
			}
			sublist.add(vertexID);
		}

		// Init workers
		for (Entry<Worker, List<Vertex>> e : divided.entrySet()) {
			currentStep.add(e.getKey().getID());
			e.getKey().createTask(input, func, taskID, initialData);
		}

		for (int i = 0; i < supersteps; i++) {

			for (Worker w : coord.getWorkers()) {
				w.nextSuperstep(i, taskID);
			}

			synchronized (this) {
				while (!currentStep.isEmpty())
					wait();
			}
			boolean finished = true;
			for (Worker w : coord.getWorkers()) {
				if (w.hasWork(taskID)) {
					currentStep.add(w.getID());
					finished = false;
				}
			}
			if (finished)
				break;
		}

		return mergeGraph();
	}

	private PregelGraph mergeGraph() throws Exception {
		PregelGraph ret = new PregelGraph();
		for (Worker w : coord.getWorkers()) {
			ret.merge(w.getResult(taskID));
		}
		return ret;
	}
}