package edu.jlime.graphly.rec.hits;

import java.util.Iterator;

import edu.jlime.pregel.PregelSubgraph;
import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatMessage;
import edu.jlime.pregel.worker.FloatAggregator;

public class HITSPregel implements VertexFunction<FloatMessage> {
	private String authKey;
	private String hubKey;

	public HITSPregel(String auth, String hub) {
		this.authKey = auth;
		this.hubKey = hub;
	}

	@Override
	public void execute(long v, Iterator<FloatMessage> in, Context ctx) throws Exception {

		PregelGraph g = ctx.getGraph();
		PregelSubgraph sg = ctx.getSubGraph("hits-sg");
		float auth = 0f;
		float hub = 0f;
		if (ctx.getSuperStep() == 0) {
			auth = 1f / sg.size();
			hub = 1f / sg.size();
		} else {

			while (in.hasNext()) {
				FloatMessage pm = in.next();
				float m = pm.value();
				if (pm.getType().equals("hits-auth"))
					hub += m;
				else
					auth += m;
			}

			float acc_auth = ((FloatAggregator) ctx.getAggregator("hits-auth")).get();
			float acc_hub = ((FloatAggregator) ctx.getAggregator("hits-hub")).get();

			auth /= acc_auth;
			hub /= acc_hub;

			g.setFloat(v, authKey, auth);
			g.setFloat(v, hubKey, hub);
		}

		long[] inc = sg.getIncoming(v);
		for (long l : inc)
			ctx.sendFloat("hits-auth", l, auth);

		long[] out = sg.getOutgoing(v);
		for (long l : out)
			ctx.sendFloat("hits-hub", l, hub);

		((FloatAggregator) ctx.getAggregator("hits-hub")).add(-1, -1, inc.length == 0 ? auth : auth * inc.length);
		((FloatAggregator) ctx.getAggregator("hits-auth")).add(-1, -1, out.length == 0 ? hub : hub * out.length);

	}
}
