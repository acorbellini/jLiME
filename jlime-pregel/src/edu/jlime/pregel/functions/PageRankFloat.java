package edu.jlime.pregel.functions;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import gnu.trove.iterator.TLongIterator;

public class PageRankFloat implements VertexFunction {

	// float error = 0.0001f;
	private int vertexSize;

	public PageRankFloat(int vSize) {
		this.vertexSize = vSize;
	}

	// Float d = 0.85;

	@Override
	public void execute(long v, List<PregelMessage> in, WorkerContext ctx)
			throws Exception {
		Logger log = Logger.getLogger(PageRankFloat.class);

		Graph graph = ctx.getGraph();

		float oldval = graph.getFloat(v, "pagerank");

		// Jacobi iterative method: (1-d) + d * function
		// Example :
		// http://mathscinotes.wordpress.com/2012/01/02/worked-pagerank-example/
		float currentVal = oldval;
		if (ctx.getSuperStep() >= 1) {
			float sum = 0f;
			for (PregelMessage pm : in) {
				// sum += Float.intBitsToFloat(DataTypeUtils
				// .byteArrayToInt((byte[]) pm.getV()));
				sum += ((FloatPregelMessage) pm).getFloat();
			}

			float d = graph.getFloat(v, "ranksource");
			currentVal = (1 - d) / vertexSize + d * (sum);
			if (log.isDebugEnabled())
				log.debug("Saving pagerank " + currentVal + " into " + v
						+ " ( 1 - " + d + "/" + graph.vertexSize() + " + " + d
						+ "*" + sum + " )");

			graph.setFloat(v, "pagerank", currentVal);

			// If converged, set as halted for the next superstep. The value of
			// the current pagerank was saved in
			// the previous step.
			// if (Math.abs(oldval - currentVal) < error)
			// ctx.setHalted();
		}

		int outgoingSize = graph.getOutgoingSize(v);

		// Dangling nodes distribute pagerank across the whole graph.

		// byte[] data =
		// DataTypeUtils.intToByteArray(Float.floatToIntBits(val));
		if (outgoingSize == 0) {
			float val = currentVal / vertexSize;
			ctx.sendAllFloat(val);
		} else {
			float val = currentVal / outgoingSize;
			TLongIterator outgoing = graph.getOutgoing(v).iterator();
			while (outgoing.hasNext()) {
				long vertex = outgoing.next();
				if (log.isDebugEnabled())
					log.debug("Sending message to " + vertex + " from " + v);
				ctx.sendFloat(vertex, val);
			}
		}
	}
}
