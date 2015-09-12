package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.graphly.traversal.Dir;
import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class KatzRootedPregel implements VertexFunction<FloatMessage> {

	private String prop;
	private float beta;
	private int lastStep;
	private Dir dir;

	public KatzRootedPregel(String val, float beta2, int last, Dir dir) {
		this.prop = val;
		this.beta = beta2;
		this.lastStep = last;
		this.dir = dir;
	}

	@Override
	public void execute(long v, Iterator<FloatMessage> in, Context ctx) throws Exception {
		PregelGraph g = ctx.getGraph();

		float adj = 0f;
		if (ctx.getSuperStep() > 0) {
			float sum = 0f;
			while (in.hasNext()) {
				FloatMessage msg = in.next();
				sum += msg.value();
			}

			float oldKatz = g.getFloat(prop, v, 0f);
			float katz = oldKatz + sum;
			g.setFloat(v, prop, katz);

			adj = sum * beta;

		} else
			adj = beta;

		if (ctx.getSuperStep() < lastStep) {
			TLongHashSet out = g.getAdjacents(v, dir);
			TLongIterator it = out.iterator();
			while (it.hasNext()) {
				long next = it.next();
				ctx.sendFloat("katz", next, adj);
			}
		}
	}
}
