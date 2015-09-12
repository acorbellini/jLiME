package edu.jlime.pregel.util;

import java.util.Iterator;

import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.PregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class DisableDangling implements VertexFunction<PregelMessage> {

	@Override
	public void execute(long v, Iterator<PregelMessage> in, Context ctx) throws Exception {
		PregelGraph graph = ctx.getGraph();
		if (ctx.getSuperStep() > 0) {
			while (in.hasNext()) {
				PregelMessage pm = in.next();
				graph.disableLink(v, pm.getFrom());
			}
		}

		if (graph.getOutgoingSize(v) == 0) {
			TLongHashSet incoming = graph.getIncoming(v);
			TLongIterator it = incoming.iterator();
			while (it.hasNext()) {
				ctx.send("dangling", it.next(), "DELETED");
			}
			graph.disable(v);
		}
	}

}
