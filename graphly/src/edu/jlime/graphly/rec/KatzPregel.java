package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.worker.FloatAggregator;
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
		PregelGraph g = ctx.getGraph();

		float adj = 0f;
		if (ctx.getSuperStep() > 0) {
			while (in.hasNext()) {
				FloatPregelMessage msg = in.next();
				adj += msg.getFloat();
			}

			float diff = 0f;
			// if (adj > 10E-10) {
			float oldKatz = g.getFloat(prop, v, 0f);
			float katz = oldKatz + adj;
			g.setFloat(v, prop, katz);

			diff = Math.abs(katz - oldKatz);
			// }
			FloatAggregator ag = (FloatAggregator) ctx.getAggregator("katz");
			ag.add(-1, -1, diff);
			adj *= beta;
		} else
			adj = beta;

		TLongHashSet out = g.getOutgoing(v);
		TLongIterator it = out.iterator();
		while (it.hasNext()) {
			long next = it.next();
			ctx.sendFloat("katz", next, adj);
		}
	}
}
