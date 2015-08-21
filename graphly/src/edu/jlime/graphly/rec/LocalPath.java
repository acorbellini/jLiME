package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class LocalPath implements VertexFunction<FloatPregelMessage> {

	private String k;
	private float alpha;

	public LocalPath(String key, float alpha) {
		this.k = key;
		this.alpha = alpha;
	}

	@Override
	public void execute(long v, Iterator<FloatPregelMessage> in,
			WorkerContext ctx) throws Exception {
		Graph g = ctx.getGraph();
		float adj = 0f;
		if (ctx.getSuperStep() > 0) {

			while (in.hasNext()) {
				FloatPregelMessage msg = in.next();
				adj += msg.getFloat();
			}

			if (ctx.getSuperStep() > 1) {
				float lp = adj;
				if (ctx.getSuperStep() == 3)
					lp = g.getFloat(k, v, 0f) + alpha * adj;
				g.setFloat(v, k, lp);
			}
		} else
			adj = 1f;

		if (ctx.getSuperStep() < 3) {
			TLongHashSet out = g.getOutgoing(v);
			TLongIterator it = out.iterator();
			while (it.hasNext()) {
				long next = it.next();
				ctx.sendFloat("lp", next, adj);
			}
		}
	}
}
