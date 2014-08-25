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

import edu.jlime.pregel.client.TaskContext;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public class WorkerTask implements TaskContext {

	private static final int MAX_THREADS = 10;

	public static class StepData {
		public HashMap<Vertex, VertexData> adyacents = new HashMap<>();
	}

	Set<Vertex> modified = new HashSet<>();

	private PregelGraph graph;

	private UUID taskid;

	private Coordinator coord;

	private HashMap<Vertex, StepData> queue = new HashMap<>();

	private VertexFunction f;

	private WorkerImpl worker;

	protected Logger log = Logger.getLogger(WorkerTask.class);

	public WorkerTask(PregelGraph input, WorkerImpl w, Coordinator coord,
			VertexFunction f, UUID taskID, HashMap<Vertex, VertexData> init) {
		this.graph = input;
		this.worker = w;
		this.coord = coord;
		this.taskid = taskID;
		this.f = f;
		for (Entry<Vertex, VertexData> e : init.entrySet()) {
			this.graph.setVal(e.getKey(), e.getValue());
			this.queue.put(e.getKey(), new StepData());
		}
	}

	public void queueVertexData(Vertex from, Vertex to, VertexData data) {
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

	public void nextStep(int superstep) throws Exception {

		ExecutorService exec = Executors.newFixedThreadPool(MAX_THREADS);

		Semaphore execCount = new Semaphore(MAX_THREADS);

		HashMap<Vertex, StepData> current = new HashMap<>(this.queue);
		this.queue.clear();
		for (Entry<Vertex, StepData> vertex : current.entrySet()) {
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

		coord.finished(taskid, this.worker.getID());
	}

	public boolean hasWork() {
		return !queue.isEmpty();
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

	@Override
	public PregelGraph getGraph() {
		return graph;
	}

	@Override
	public void send(Vertex from, Vertex to, VertexData data) throws Exception {
		worker.getWorker(to).sendDataToVertex(from, to, data, taskid);
	}
}
