package edu.jlime.graphly.rec.salsa;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.MultiStepVertexFunction;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.messages.GenericPregelMessage;
import edu.jlime.pregel.messages.PregelMessage;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class SALSAPregel implements VertexFunction {
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
	private MultiStepVertexFunction steps;

	public SALSAPregel(final String authKey, final String hubKey,
			final int gSize) {
		this.gSize = gSize;
		this.steps = new MultiStepVertexFunction();
		this.steps.step(new VertexFunction() {

			@Override
			public void execute(long v, Iterator<PregelMessage> in,
					WorkerContext ctx) throws Exception {
				Graph graph = ctx.getGraph();
				float auth = 0f;
				float hub = 0f;
				if (ctx.getSuperStep() > 0) {
					while (in.hasNext()) {
						GenericPregelMessage<SalsaMessage> msg = (GenericPregelMessage<SalsaMessage>) in
								.next();
						SalsaMessage m = msg.getV();
						auth += m.auth;
						hub += m.hub;
					}
					graph.setFloat(v, authKey, auth);
					graph.setFloat(v, hubKey, hub);
				} else {
					auth = graph.getFloat(v, authKey);
					hub = graph.getFloat(v, hubKey);
				}
				{
					TLongArrayList incoming = graph.getIncoming(v);
					if (!incoming.isEmpty()) {
						SalsaMessage authM = new SalsaMessage(auth
								/ incoming.size(), 0);
						TLongIterator it = incoming.iterator();
						while (it.hasNext()) {
							long inV = it.next();
							ctx.send("salsa", inV, authM);
						}
					} else {
						SalsaMessage authM = new SalsaMessage(auth / gSize, 0);
						ctx.sendAll("salsa", authM);
					}
				}
				{
					TLongArrayList outcoming = graph.getOutgoing(v);
					if (!outcoming.isEmpty()) {
						SalsaMessage hubM = new SalsaMessage(0, hub
								/ outcoming.size());
						TLongIterator itOut = outcoming.iterator();
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
		}).step(new VertexFunction() {
			@Override
			public void execute(long v, Iterator<PregelMessage> in,
					WorkerContext ctx) throws Exception {
				float sumAuth = 0f;
				float sumHub = 0f;
				while (in.hasNext()) {
					GenericPregelMessage<SalsaMessage> msg = (GenericPregelMessage<SalsaMessage>) in
							.next();
					SalsaMessage m = msg.getV();
					sumAuth += m.auth;
					sumHub += m.hub;
				}
				Graph graph = ctx.getGraph();
				{
					TLongArrayList incoming = graph.getIncoming(v);
					if (!incoming.isEmpty()) {
						SalsaMessage hub = new SalsaMessage(0, sumHub
								/ incoming.size());
						TLongIterator it = incoming.iterator();
						while (it.hasNext()) {
							long inVertex = it.next();
							ctx.send("salsa", inVertex, hub);
						}
					} else {
						SalsaMessage hub = new SalsaMessage(0, sumHub / gSize);
						ctx.sendAll("salsa", hub);
					}
				}
				{
					TLongArrayList outcoming = graph.getOutgoing(v);
					if (!outcoming.isEmpty()) {
						SalsaMessage auth = new SalsaMessage(sumAuth
								/ outcoming.size(), 0);
						TLongIterator itOut = outcoming.iterator();
						while (itOut.hasNext()) {
							long outVertex = itOut.next();
							ctx.send("salsa", outVertex, auth);
						}

					} else {
						SalsaMessage auth = new SalsaMessage(sumAuth / gSize, 0);
						ctx.sendAll("salsa", auth);
					}
				}
			}
		});
	}

	@Override
	public void execute(long v, Iterator<PregelMessage> in, WorkerContext ctx)
			throws Exception {
		steps.execute(v, in, ctx);
	}
}
