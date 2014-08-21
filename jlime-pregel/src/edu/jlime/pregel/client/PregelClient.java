package edu.jlime.pregel.client;

import java.util.Arrays;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class PregelClient {
	private PregelGraph input;
	private RPCDispatcher rpc;

	public PregelClient(PregelGraph graph) throws Exception {
		this.input = graph;
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		this.rpc = new JlimeFactory(config).build();
	}

	public PregelGraph execute(Vertex id, VertexFunction func) throws Exception {
		Coordinator coord = rpc.manage(
				new CoordinatorFactory(rpc, "coordinator")).first();
		coord.setGraph(input);

		WorkerBroadcast broadcastWorker = rpc.manage(
				new WorkerFactory(rpc, "worker")).broadcast();
		broadcastWorker.setGraph(input);

		return coord.execute(Arrays.asList(new Vertex[] { id }), func);
	}
}
