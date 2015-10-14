package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.graphly.traversal.Dir;
import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class LocalPath implements VertexFunction<FloatMessage> {

	private String k;
	private float alpha;
	private Dir dir;

	public LocalPath(String key, float alpha, Dir dir) {
		this.k = key;
		this.alpha = alpha;
		this.dir = dir;
	}

	@Override
	public void execute(long v, Iterator<FloatMessage> in, Context ctx) throws Exception {
		PregelGraph g = ctx.getGraph();
		float adj = 0f;
		if (ctx.getSuperStep() > 0) {
			while (in.hasNext()) {
				FloatMessage msg = in.next();
				adj += msg.value();
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
			TLongHashSet out = g.getAdjacents(v, dir);
			TLongIterator it = out.iterator();
			while (it.hasNext()) {
				long next = it.next();
				ctx.sendFloat("lp", next, adj);
			}

		}
	}
}
