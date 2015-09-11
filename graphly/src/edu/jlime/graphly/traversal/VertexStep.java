package edu.jlime.graphly.traversal;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
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

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		TLongHashSet vertices = before.vertices();
		final List<Pair<Node, TLongArrayList>> div = map.map(Graphly.NUM_JOBS, vertices.toArray(), ctx);

		ForkJoinTask<long[]> fj = new ForkJoinTask<>();
		for (Pair<Node, TLongArrayList> e : div) {
			fj.putJob(new VertexJob(tr.getGraph(), dir, max_edges, e.getValue().toArray()), e.getKey());
		}

		if (log.isDebugEnabled())
			log.debug("Executing " + div.size() + " jobs.");

		TLongHashSet finalRes = fj.execute(new ResultListener<long[], TLongHashSet>() {
			TLongHashSet ret = new TLongHashSet();

			AtomicInteger cont = new AtomicInteger(div.size());

			@Override
			public synchronized void onSuccess(long[] result) {
				if (log.isDebugEnabled())
					log.debug("Received result jobs, remaining " + cont.decrementAndGet());
				ret.addAll(result);
			}

			@Override
			public TLongHashSet onFinished() {
				return ret;
			}

			@Override
			public void onFailure(Exception res) {
			}
		});
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
