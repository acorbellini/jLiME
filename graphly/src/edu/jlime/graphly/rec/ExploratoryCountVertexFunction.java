package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class ExploratoryCountVertexFunction implements VertexFunction<FloatPregelMessage> {

	@Override
	public void execute(long v, Iterator<FloatPregelMessage> in, WorkerContext ctx) throws Exception {
		PregelGraph graph = ctx.getGraph();
		// INIT
		if (ctx.getSuperStep() == 0) {
			graph.setVal(v, "type", "S");
			TLongHashSet out = graph.getOutgoing(v);
			TLongIterator it = out.iterator();
			while (it.hasNext()) {
				long vout = it.next();
				ctx.sendFloat("ec", vout, 1f);
			}
		}
		// S group
		else if (ctx.getSuperStep() == 1) {
			float sum = 0f;
			while (in.hasNext()) {
				FloatPregelMessage msg = (FloatPregelMessage) in.next();
				sum += msg.getFloat();
			}
			graph.setVal(v, "type", "S");
			TLongHashSet incoming = graph.getIncoming(v);
			TLongIterator it = incoming.iterator();
			while (it.hasNext()) {
				long vin = it.next();
				ctx.sendFloat("ec", vin, sum);
			}
		} else {
			Object type = graph.get(v, "type");
			if (type != null && type.equals("S"))
				return;

			float sum = 0f;
			while (in.hasNext()) {
				FloatPregelMessage msg = (FloatPregelMessage) in.next();
				sum += msg.getFloat();
			}

			if (ctx.getSuperStep() == 2) {
				TLongHashSet outgoing = graph.getOutgoing(v);
				TLongIterator it = outgoing.iterator();
				while (it.hasNext()) {
					long vout = it.next();
					ctx.sendFloat("ec", vout, sum);
				}
			} // Target Group
			else if (ctx.getSuperStep() == 3) {
				graph.setFloat(v, "ec", sum);
			}
		}
	}
}
