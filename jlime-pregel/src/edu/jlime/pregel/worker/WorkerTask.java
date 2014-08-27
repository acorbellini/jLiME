package edu.jlime.pregel.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public class WorkerTask implements WorkerContext {

	private static final int MAX_THREADS = 10;

	Set<Vertex> modified = new HashSet<>();

	private PregelGraph graph;

	private UUID taskid;

	private Coordinator coord;

	private HashMap<Vertex, HashSet<PregelMessage>> queue = new HashMap<>();

	private VertexFunction f;

	private WorkerImpl worker;

	protected Logger log = Logger.getLogger(WorkerTask.class);

	private HashSet<Vertex> halted = new HashSet<>();

	private int currentStep;

	public WorkerTask(PregelGraph input, WorkerImpl w, Coordinator coord,
			VertexFunction f, UUID taskID, HashSet<Vertex> init) {
		this.graph = input;
		this.worker = w;
		this.coord = coord;
		this.taskid = taskID;
		this.f = f;
		for (Vertex e : init) {
			this.queue.put(e, new HashSet<>());
		}
	}

	public void queueVertexData(Vertex from, Vertex to, VertexData data) {
		if (halted.contains(to))
			return;

		HashSet<PregelMessage> vertexData = queue.get(to);
		if (vertexData == null) {
			synchronized (this) {
				vertexData = queue.get(to);
				if (vertexData == null) {
					vertexData = new HashSet<>();
					queue.put(to, vertexData);
				}
			}
		}
		vertexData.add(new PregelMessage(from, data));
	}

	public void nextStep(int superstep) throws Exception {
		this.currentStep = superstep;

		if (queue.isEmpty()) {
			coord.finished(taskid, this.worker.getID(), false);
			return;
		}
		ExecutorService exec = Executors.newFixedThreadPool(MAX_THREADS);

		Semaphore execCount = new Semaphore(MAX_THREADS);

		HashMap<Vertex, HashSet<PregelMessage>> current = new HashMap<>(
				this.queue);
		this.queue.clear();
		for (Entry<Vertex, HashSet<PregelMessage>> vertex : current.entrySet()) {
			modified.add(vertex.getKey());
			execCount.acquire();
			exec.execute(new Runnable() {
				@Override
				public void run() {
					try {
						if (log.isDebugEnabled())
							log.debug("Executing function on vertex "
									+ vertex.getKey());
						f.execute(vertex.getKey(), vertex.getValue(),
								WorkerTask.this);
						if (log.isDebugEnabled())
							log.debug("Finished executing function on vertex "
									+ vertex.getKey());
					} catch (Exception e) {
						e.printStackTrace();
					}
					execCount.release();
				}
			});
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		coord.finished(taskid, this.worker.getID(), true);
	}

	public PregelGraph getResultGraph() {
		PregelGraph ret = new PregelGraph();
		for (Vertex vertex : modified) {
			Set<Vertex> adyacency = graph.getOutgoing(vertex);
			if (adyacency != null)
				for (Vertex edge : adyacency) {
					ret.putLink(vertex, edge);
				}
			VertexData data = graph.getData(vertex);
			ret.setVal(vertex, data);

			ret.addVertex(vertex);
		}
		return ret;
	}

	@Override
	public PregelGraph getGraph() {
		return graph;
	}

	@Override
	public void send(Vertex from, Vertex to, VertexData data) throws Exception {
		worker.getWorker(to).sendDataToVertex(from, to, data, taskid);
	}

	@Override
	public void setHalted(Vertex v) {
		this.halted.add(v);

	}

	@Override
	public Integer getSuperStep() {
		return currentStep;
	}
}
