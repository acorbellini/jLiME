package edu.jlime.graphly.rec.salsa;

import java.util.Iterator;

import edu.jlime.pregel.PregelSubgraph;
import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatMessage;

public class SALSAPregel implements VertexFunction<FloatMessage> {

	private String authKey;
	private String hubKey;
	private int as;
	private int hs;

	public SALSAPregel(final String authKey, final String hubKey, int as,
			int hs) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.as = as;
		this.hs = hs;
	}

	@Override
	public void execute(long v, Iterator<FloatMessage> in, Context ctx)
			throws Exception {
		PregelGraph graph = ctx.getGraph();
		float auth = 0f;
		float hub = 0f;
		Integer superstep = ctx.getSuperStep();
		PregelSubgraph subgraph = ctx.getSubGraph("salsa-sg");

		if (superstep == 0) {
			auth = 1f / as;
			hub = 1f / hs;
		} else {
			while (in.hasNext()) {
				FloatMessage msg = in.next();
				if (msg.getType().equals("salsa-auth"))
					auth += msg.value();
				else
					hub += msg.value();
			}

			if (superstep % 2 == 0) {
				graph.setFloat(v, authKey, auth);
				graph.setFloat(v, hubKey, hub);
			}
		}

		// Send auth data
		if (auth > 0) {
			long[] toSendAuth = superstep % 2 == 0 ? subgraph.getIncoming(v)
					: subgraph.getOutgoing(v);
			if (toSendAuth.length > 0) {
				float toSend = auth / toSendAuth.length;
				for (long inV : toSendAuth) {
					ctx.sendFloat("salsa-auth", inV, toSend);
				}
			}
		}

		// Send hub data.
		if (hub > 0) {
			long[] toSendHub = superstep % 2 == 0 ? subgraph.getOutgoing(v)
					: subgraph.getIncoming(v);
			if (toSendHub.length > 0) {
				float toSend = hub / toSendHub.length;
				for (long outV : toSendHub) {
					ctx.sendFloat("salsa-hub", outV, toSend);
				}
			}
		}
	}
}
