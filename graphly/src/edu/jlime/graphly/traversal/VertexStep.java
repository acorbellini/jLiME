package edu.jlime.graphly.traversal;

import org.apache.log4j.Logger;

import gnu.trove.set.hash.TLongHashSet;

public class VertexStep implements Step {

	private Dir dir;
	private Traversal tr;
	private int max_edges;
	private boolean expand;

	public VertexStep(Dir dir, int max_edges, Traversal tr) {
		this(dir, max_edges, false, tr);
	}

	public VertexStep(Dir dir, int max_edges, boolean b, Traversal tr) {
		this.dir = dir;
		this.tr = tr;
		this.max_edges = max_edges;
		this.expand = b;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {

		final Logger log = Logger.getLogger(VertexStep.class);

		TLongHashSet vertices = before.vertices();
		TLongHashSet finalRes = new TLongHashSet(tr.getGraph().getEdgesMax(dir, max_edges, vertices.toArray()));

		// JobContext ctx =
		// jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());
		//
		// TLongHashSet vertices = before.vertices();
		//
		// final List<Pair<Node, TLongArrayList>> div =
		// map.map(Graphly.NUM_JOBS, vertices.toArray(), ctx);
		//
		// ForkJoinTask<long[]> fj = new ForkJoinTask<>();
		// for (Pair<Node, TLongArrayList> e : div) {
		// fj.putJob(new VertexJob(tr.getGraph(), dir, max_edges,
		// e.getValue().toArray()), e.getKey());
		// }
		//
		// if (log.isDebugEnabled())
		// log.debug("Executing " + div.size() + " jobs.");
		//
		// TLongHashSet finalRes = fj.execute(new ResultListener<long[],
		// TLongHashSet>() {
		// TLongHashSet ret = new TLongHashSet();
		//
		// AtomicInteger cont = new AtomicInteger(div.size());
		//
		// @Override
		// public synchronized void onSuccess(long[] result) {
		// if (log.isDebugEnabled())
		// log.debug("Received result jobs, remaining " +
		// cont.decrementAndGet());
		// ret.addAll(result);
		// }
		//
		// @Override
		// public TLongHashSet onFinished() {
		// return ret;
		// }
		//
		// @Override
		// public void onFailure(Exception res) {
		// }
		// });
		log.info("Returning  " + finalRes.size() + " vertices.");
		if (expand)
			finalRes.addAll(vertices);
		return new VertexResult(finalRes);
	}

	@Override
	public String toString() {
		return "VertexStep [dir=" + dir + ", max_edges=" + max_edges + "]";
	}

}
