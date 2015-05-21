package edu.jlime.pregel.util;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.PregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class DisableDangling implements VertexFunction {

	@Override
	public void execute(long v, Iterator<PregelMessage> in, WorkerContext ctx)
			throws Exception {
		Graph graph = ctx.getGraph();
		if (ctx.getSuperStep() > 0) {
			while (in.hasNext()) {
				PregelMessage pm = in.next();
				graph.disableLink(v, pm.getFrom());
			}
		}

		if (graph.getOutgoingSize(v) == 0) {
			TLongArrayList incoming = graph.getIncoming(v);
			TLongIterator it = incoming.iterator();
			while (it.hasNext()) {
				ctx.send("dangling", it.next(), "DELETED");
			}
			graph.disable(v);
		}
	}

}
