package edu.jlime.pregel.coordinator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.VertexData;
import edu.jlime.pregel.worker.rpc.Worker;

public class CoordinatorTask {
	UUID taskID = UUID.randomUUID();

	private Set<UUID> currentStep = new HashSet<>();

	private CoordinatorImpl coord;

	private Logger log = Logger.getLogger(CoordinatorTask.class);

	public CoordinatorTask(CoordinatorImpl coordinatorImpl) {
		this.coord = coordinatorImpl;
	}

	public synchronized void finished(UUID workerID) {
		if (log.isDebugEnabled())
			log.debug("Received finished  from worker : " + workerID);
		currentStep.remove(workerID);
		notify();
		if (log.isDebugEnabled())
			log.debug("Remaining in step: " + currentStep.size());
	}

	public PregelGraph execute(PregelGraph input,
			HashMap<Vertex, VertexData> initialData, VertexFunction func,
			int supersteps) throws Exception {

		// Divide vertex list.
		HashMap<Worker, HashMap<Vertex, VertexData>> divided = new HashMap<>();
		for (Vertex vertexID : initialData.keySet()) {
			Worker worker = coord.getWorker(vertexID);
			HashMap<Vertex, VertexData> sublist = divided.get(worker);
			if (sublist == null) {
				sublist = new HashMap<>();
				divided.put(worker, sublist);
			}
			sublist.put(vertexID, initialData.get(vertexID));
		}

		// Init workers
		// for (Entry<Worker, HashMap<Vertex, byte[]>> e : divided.entrySet()) {
		for (Worker w : coord.getWorkers()) {
			currentStep.add(w.getID());
			if (divided.containsKey(w))
				w.createTask(input, func, taskID, divided.get(w));
			else
				w.createTask(input, func, taskID, new HashMap<>());
		}

		for (int i = 0; i < supersteps; i++) {
			if (log.isDebugEnabled())
				log.debug("Running superstep " + i + " Remaining "
						+ currentStep.size());
			for (Worker w : coord.getWorkers()) {
				w.nextSuperstep(i, taskID);
			}

			synchronized (this) {
				while (!currentStep.isEmpty())
					wait();
			}

			// if (log.isDebugEnabled())
			log.info("Finished superstep " + i);

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
		log.info("Finished");
		return mergeGraph();
	}

	private PregelGraph mergeGraph() throws Exception {
		if (log.isDebugEnabled())
			log.debug("Merging Graph");
		PregelGraph ret = new PregelGraph();
		for (Worker w : coord.getWorkers())
			ret.merge(w.getResult(taskID));
		return ret;
	}
}