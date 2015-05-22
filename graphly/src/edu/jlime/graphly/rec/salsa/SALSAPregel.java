package edu.jlime.graphly.rec.salsa;

import java.util.Iterator;

import edu.jlime.graphly.rec.salsa.SALSAPregel.SalsaMessage;
import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.GenericPregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class SALSAPregel implements
		VertexFunction<GenericPregelMessage<SalsaMessage>> {
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

	private float gSize;
	private String authKey;
	private String hubKey;

	public SALSAPregel(final String authKey, final String hubKey,
			final int gSize) {
		this.gSize = gSize;
		this.authKey = authKey;
		this.hubKey = hubKey;
	}

	@Override
	public void execute(long v,
			Iterator<GenericPregelMessage<SalsaMessage>> in, WorkerContext ctx)
			throws Exception {
		Graph graph = ctx.getGraph();
		float auth = 0f;
		float hub = 0f;
		Integer superstep = ctx.getSuperStep();
		if (superstep == 0) {
			auth = graph.getFloat(v, authKey);
			hub = graph.getFloat(v, hubKey);
		} else if (superstep > 0 && superstep % 2 == 0) {
			while (in.hasNext()) {
				GenericPregelMessage<SalsaMessage> msg = in.next();
				SalsaMessage m = msg.getV();
				auth += m.auth;
				hub += m.hub;
			}
			graph.setFloat(v, authKey, auth);
			graph.setFloat(v, hubKey, hub);
		} else {
			while (in.hasNext()) {
				GenericPregelMessage<SalsaMessage> msg = in.next();
				SalsaMessage m = msg.getV();
				auth += m.auth;
				hub += m.hub;
			}
		}

		// Send auth data
		{
			TLongArrayList toSendAuth = superstep % 2 == 0 ? graph
					.getIncoming(v) : graph.getOutgoing(v);
			if (!toSendAuth.isEmpty()) {
				SalsaMessage authM = new SalsaMessage(auth / toSendAuth.size(),
						0);
				TLongIterator it = toSendAuth.iterator();
				while (it.hasNext()) {
					long inV = it.next();
					ctx.send("salsa", inV, authM);
				}
			} else {
				SalsaMessage authM = new SalsaMessage(auth / gSize, 0);
				ctx.sendAll("salsa", authM);
			}
		}

		// Send hub data.
		{
			TLongArrayList toSendHub = superstep % 2 == 0 ? graph
					.getOutgoing(v) : graph.getIncoming(v);
			if (!toSendHub.isEmpty()) {
				SalsaMessage hubM = new SalsaMessage(0, hub / toSendHub.size());
				TLongIterator itOut = toSendHub.iterator();
				while (itOut.hasNext()) {
					long outV = itOut.next();
					ctx.send("salsa", outV, hubM);
				}
			} else {
				SalsaMessage hubM = new SalsaMessage(0, hub / gSize);
				ctx.sendAll("salsa", hubM);
			}
		}
	}
}
