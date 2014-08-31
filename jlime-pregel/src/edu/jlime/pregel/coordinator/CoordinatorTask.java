package edu.jlime.pregel.coordinator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;

public class CoordinatorTask {
	UUID taskID = UUID.randomUUID();

	private volatile boolean finished = true;

	private Set<UUID> currentStep = new HashSet<>();

	private CoordinatorImpl coord;

	private Logger log = Logger.getLogger(CoordinatorTask.class);

	private HashMap<String, Aggregator> aggregators;

	public CoordinatorTask(CoordinatorImpl coordinatorImpl,
			HashMap<String, Aggregator> aggs) {
		this.coord = coordinatorImpl;
		this.aggregators = aggs;
	}

	public synchronized void finished(UUID workerID, Boolean didWork) {
		if (didWork)
			finished = false;

		// if (log.isDebugEnabled())
		log.info("Received finished  from worker : " + workerID);
		currentStep.remove(workerID);
		notifyAll();
		// if (log.isDebugEnabled())
		log.info("Remaining in step: " + currentStep.size());
	}

	public PregelGraph execute(PregelGraph input, List<Vertex> vertex,
			VertexFunction func, int supersteps) throws Exception {

		// Divide vertex list.
		HashMap<Worker, HashSet<Vertex>> divided = new HashMap<>();
		for (Vertex vertexID : vertex) {
			Worker worker = coord.getWorker(vertexID);
			HashSet<Vertex> sublist = divided.get(worker);
			if (sublist == null) {
				sublist = new HashSet<>();
				divided.put(worker, sublist);
			}
			sublist.add(vertexID);
		}

		// Init workers
		// for (Entry<Worker, HashMap<Vertex, byte[]>> e : divided.entrySet()) {
		for (Worker w : coord.getWorkers()) {
			currentStep.add(w.getID());
			if (divided.containsKey(w))
				w.createTask(input, func, taskID, divided.get(w));
			else
				w.createTask(input, func, taskID, new HashSet<>());
		}

		for (int i = 0; i < supersteps; i++) {
			finished = true;

			// if (log.isDebugEnabled())
			log.info("Running superstep " + i + " Remaining "
					+ currentStep.size());

			coord.getWorkerBroadcast().nextSuperstep(i, taskID);

			synchronized (this) {
				while (!currentStep.isEmpty())
					wait();
			}

			// if (log.isDebugEnabled())
			log.info("Finished superstep " + i);

			if (finished)
				break;

			for (Worker w : coord.getWorkers())
				currentStep.add(w.getID());

			for (Entry<String, Aggregator> e : aggregators.entrySet()) {
				e.getValue().superstep(i);
			}

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

	public Double getAggregatedValue(Vertex v, String k) {
		log.info("Obtaining aggregated value from " + k + " for " + v);
		Aggregator a = aggregators.get(k);
		if (a != null) {

			Double val = a.getVal(v);
			log.info("Obtained " + k + " for " + v + ": " + val);
			return val;
		}
		log.info("No aggregator by that name: " + k);
		return null;
	}

	public void setAggregatedValue(Vertex v, String name, Double val) {
		Aggregator a = aggregators.get(name);
		if (a != null)
			a.setVal(v, val);
	}
}