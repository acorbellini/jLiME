package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.graphly.traversal.Dir;
import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class FriendLink implements VertexFunction<FloatPregelMessage> {

	private String prop;
	private long vcount;
	private int lastStep;
	private Dir dir;

	public FriendLink(String k, long vcount, int lastStep, Dir dir) {
		this.prop = k;
		this.vcount = vcount;
		this.lastStep = lastStep;
		this.dir = dir;
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
				float prod = 1f;
				for (int i = 2; i <= ctx.getSuperStep(); i++) {
					prod *= vcount - i;
				}
				float fl = g.getFloat(prop, v, 0f)
						+ (1 / (float) ctx.getSuperStep()) * (1 / prod) * adj;

				g.setFloat(v, prop, fl);
			}
		} else
			adj = 1f;

		if (ctx.getSuperStep() < lastStep) {
			TLongHashSet out = g.getAdjacents(v, dir);
			TLongIterator it = out.iterator();
			while (it.hasNext()) {
				long next = it.next();
				ctx.sendFloat("fl", next, adj);
			}
		}
	}

}
