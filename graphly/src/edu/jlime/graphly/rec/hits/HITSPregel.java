package edu.jlime.graphly.rec.hits;

import java.io.Serializable;
import java.util.Iterator;

import edu.jlime.graphly.rec.hits.HITSPregel.HITSMessage;
import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.GenericPregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class HITSPregel implements
		VertexFunction<GenericPregelMessage<HITSMessage>> {
	public static final class HITSMerger extends
			ObjectMessageMerger<HITSMessage> {
		@Override
		public void merge(HITSMessage f, HITSMessage t) {
			t.a = t.a + f.a;
			t.h = t.h + f.h;
		}

		@Override
		public HITSMessage getCopy(HITSMessage m) {
			return HITSMessage.copy(m);
		}
	}

	public static class HITSMessage implements Serializable {
		public float a;
		public float h;

		private HITSMessage(float a, float h) {
			super();
			this.a = a;
			this.h = h;
		}

		private static HITSMessage auth(float a) {
			return new HITSMessage(a, 0);
		}

		private static HITSMessage hub(float h) {
			return new HITSMessage(0, h);
		}

		public static HITSMessage copy(HITSMessage m) {
			return new HITSMessage(m.a, m.h);
		}
	}

	private String authKey;
	private String hubKey;
	private int gSize;

	public HITSPregel(String auth, String hub, int graphSize) {
		this.authKey = auth;
		this.hubKey = hub;
		this.gSize = graphSize;
	}

	@Override
	public void execute(long v, Iterator<GenericPregelMessage<HITSMessage>> in,
			WorkerContext ctx) throws Exception {

		Graph g = ctx.getGraph();

		float auth = 0f;
		float hub = 0f;
		if (ctx.getSuperStep() > 0) {
			while (in.hasNext()) {
				GenericPregelMessage<HITSMessage> pm = in.next();
				HITSMessage m = pm.getV();
				// This is switched on purpose, the algorithm sums hub messages
				// into auth and auth messages into hub.
				hub += m.a;
				auth += m.h;
			}
			g.setFloat(v, authKey, auth);
			g.setFloat(v, hubKey, hub);
		} else {
			auth = g.getFloat(v, authKey);
			hub = g.getFloat(v, hubKey);
		}

		{
			HITSMessage authM = HITSMessage.auth(auth);

			TLongArrayList inc = g.getIncoming(v);
			if (!inc.isEmpty()) {
				TLongIterator it = inc.iterator();
				while (it.hasNext())
					ctx.send("hits", it.next(), authM);
			} else {
				authM.a = auth / gSize;
				ctx.sendAll("hits", authM);
			}
		}

		{
			HITSMessage hubM = HITSMessage.hub(hub);
			TLongArrayList out = g.getOutgoing(v);
			if (!out.isEmpty()) {
				TLongIterator itOut = out.iterator();
				while (itOut.hasNext())
					ctx.send("hits", itOut.next(), hubM);
			} else {
				hubM.h = hub / gSize;
				ctx.sendAll("hits", hubM);
			}
		}

	}
}
