package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class ExploratoryCount implements VertexFunction<FloatMessage> {

	@Override
	public void execute(long v, Iterator<FloatMessage> in, Context ctx) throws Exception {
		PregelGraph graph = ctx.getGraph();
		Integer superStep = ctx.getSuperStep();

		float pathLength = 0f;
		if (superStep == 0)
			pathLength = 1f; // The target user sends a count of 1
		else {
			// Filter user and followees
			Object type = graph.get(v, "type");
			if (type != null && type.equals("filtered"))
				return;

			// Process the count of paths
			while (in.hasNext()) {
				FloatMessage msg = in.next();
				pathLength += msg.value();
			}

			// Save the results on the last step.
			if (superStep == 3) {
				graph.setFloat(v, "ec", pathLength);
				return;
			}
		}

		// Mark target and followees
		if (superStep == 0 || superStep == 1)
			graph.setVal(v, "type", "filtered");

		TLongHashSet userList = null;
		if (superStep % 2 == 0)
			userList = graph.getOutgoing(v);
		else
			userList = graph.getIncoming(v);

		// Send sums to adjacent vertices
		TLongIterator it = userList.iterator();
		while (it.hasNext()) {
			long user = it.next();
			ctx.sendFloat("ec", user, pathLength);
		}

	}
}
