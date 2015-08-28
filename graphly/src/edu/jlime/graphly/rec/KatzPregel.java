package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class KatzPregel implements VertexFunction<FloatPregelMessage> {

	private String prop;
	private float beta;

	public KatzPregel(String val, float beta) {
		this.prop = val;
		this.beta = beta;
	}

	@Override
	public void execute(long v, Iterator<FloatPregelMessage> in, WorkerContext ctx) throws Exception {
		Graph g = ctx.getGraph();

		float adj = 0f;
		if (ctx.getSuperStep() > 0) {
			while (in.hasNext()) {
				FloatPregelMessage msg = in.next();
				adj += msg.getFloat();
			}
			float katz = g.getFloat(prop, v, 0f) + (float) Math.pow(beta, ctx.getSuperStep()) * adj;
			g.setFloat(v, prop, katz);
		} else
			adj = 1f;

		TLongHashSet out = g.getOutgoing(v);
		TLongIterator it = out.iterator();
		while (it.hasNext()) {
			long next = it.next();
			ctx.sendFloat("katz", next, adj);
		}
	}
}
