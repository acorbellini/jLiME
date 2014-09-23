package edu.jlime.pregel;

import java.util.List;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.PregelMessage;

public class DisableDangling implements VertexFunction {

	@Override
	public void execute(long v, List<PregelMessage> in, WorkerContext ctx)
			throws Exception {
		Graph graph = ctx.getGraph();
		if (ctx.getSuperStep() > 0) {
			for (PregelMessage pm : in) {
				graph.disableLink(v, pm.getFrom());
			}
		}

		if (graph.getOutgoingSize(v) == 0) {
			for (Long incoming : graph.getIncoming(v)) {
				ctx.send(incoming, "DELETED");
			}
			graph.disable(v);
		}
	}

}
