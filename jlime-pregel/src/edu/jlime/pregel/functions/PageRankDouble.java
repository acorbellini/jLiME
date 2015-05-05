package edu.jlime.pregel.functions;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.DoublePregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import gnu.trove.iterator.TLongIterator;

public class PageRankDouble implements VertexFunction {

	// double error = 0.0001;
	private int vertexSize;

	public PageRankDouble(int vSize) {
		this.vertexSize = vSize;
	}

	// double d = 0.85;

	@Override
	public void execute(long v, Iterator<PregelMessage> in, WorkerContext ctx)
			throws Exception {
		Logger log = Logger.getLogger(PageRankDouble.class);

		Graph graph = ctx.getGraph();

		double oldval = graph.getDouble(v, "pagerank");

		// Jacobi iterative method: (1-d) + d * function
		// Example :
		// http://mathscinotes.wordpress.com/2012/01/02/worked-pagerank-example/
		double currentVal = oldval;
		if (ctx.getSuperStep() >= 1) {
			double sum = 0f;
			while (in.hasNext()) {
				PregelMessage pm = in.next();
				// sum += Float.intBitsToFloat(DataTypeUtils
				// .byteArrayToInt((byte[]) pm.getV()));
				sum += ((DoublePregelMessage) pm).getDouble();
			}

			double d = graph.getDouble(v, "ranksource");
			currentVal = (1 - d) / vertexSize + d * (sum);
			if (log.isDebugEnabled())
				log.debug("Saving pagerank " + currentVal + " into " + v
						+ " ( 1 - " + d + "/" + graph.vertexSize() + " + " + d
						+ "*" + sum + " )");

			graph.setDouble(v, "pagerank", currentVal);

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
			double val = currentVal / vertexSize;
			ctx.sendAllDouble(val);
		} else {
			double val = currentVal / outgoingSize;
			TLongIterator outgoing = graph.getOutgoing(v).iterator();
			while (outgoing.hasNext()) {
				long vertex = outgoing.next();
				if (log.isDebugEnabled())
					log.debug("Sending message to " + vertex + " from " + v);
				ctx.sendDouble(vertex, val);
			}
		}
	}
}
