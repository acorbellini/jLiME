package edu.jlime.graphly.rec.salsa;

import java.util.Iterator;

import edu.jlime.pregel.PregelSubgraph;
import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.FloatPregelMessage;

public class SALSAPregel implements VertexFunction<FloatPregelMessage> {

	public static class SalsaMessage {
		float auth;
		float hub;

		public SalsaMessage(float auth, float hub) {
			this.auth = auth;
			this.hub = hub;
		}
	}

	public static class SalsaMerger extends ObjectMessageMerger<SalsaMessage> {
		@Override
		public SalsaMessage getCopy(SalsaMessage msg) {
			return new SalsaMessage(msg.auth, msg.hub);
		}

		@Override
		public void merge(SalsaMessage from, SalsaMessage into) {
			into.auth = into.auth + from.auth;
			into.hub = into.hub + from.hub;
		}
	}

	private String authKey;
	private String hubKey;
	private int as;
	private int hs;

	public SALSAPregel(final String authKey, final String hubKey, int as, int hs) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.as = as;
		this.hs = hs;
	}

	@Override
	public void execute(long v, Iterator<FloatPregelMessage> in,
			WorkerContext ctx) throws Exception {
		Graph graph = ctx.getGraph();
		float auth = 0f;
		float hub = 0f;
		Integer superstep = ctx.getSuperStep();
		PregelSubgraph subgraph = ctx.getSubGraph("salsa-sg");

		if (superstep == 0) {
			auth = 1f / as;
			hub = 1f / hs;
		} else {
			while (in.hasNext()) {
				FloatPregelMessage msg = in.next();
				if (msg.getType().equals("salsa-auth"))
					auth += msg.getFloat();
				else
					hub += msg.getFloat();
			}
			
			if (superstep % 2 == 0) {
				graph.setFloat(v, authKey, auth);
				graph.setFloat(v, hubKey, hub);
			}
		}

		// Send auth data
		if (auth > 0) {
			long[] toSendAuth = superstep % 2 == 0 ? subgraph.loadIn(v)
					: subgraph.loadOut(v);
			if (toSendAuth.length > 0) {
				float toSend = auth / toSendAuth.length;
				for (long inV : toSendAuth) {
					ctx.sendFloat("salsa-auth", inV, toSend);
				}
			}
		}

		// Send hub data.
		if (hub > 0) {
			long[] toSendHub = superstep % 2 == 0 ? subgraph.loadOut(v)
					: subgraph.loadIn(v);
			if (toSendHub.length > 0) {
				float toSend = hub / toSendHub.length;
				for (long outV : toSendHub) {
					ctx.sendFloat("salsa-hub", outV, toSend);
				}
			}
		}
	}

}
