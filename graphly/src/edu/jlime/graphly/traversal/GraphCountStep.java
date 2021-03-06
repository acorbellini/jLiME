package edu.jlime.graphly.traversal;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Beta;
import edu.jlime.graphly.rec.GraphCount;
import edu.jlime.graphly.storenode.StoreNodeImpl;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.graphly.util.Gather;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class GraphCountStep implements Step {

	public static class BetaGather implements Gather<Void> {
		private int depth;
		private String k;
		private String kBeta;
		private Beta calc;

		public BetaGather(int depth, String k, String kBeta, Beta calc) {
			super();
			this.depth = depth;
			this.k = k;
			this.kBeta = kBeta;
			this.calc = calc;
		}

		@Override
		public Void gather(String graph, StoreNodeImpl node) throws Exception {
			TLongFloatIterator it = node.getFloatIterator(graph, k);
			while (it.hasNext()) {
				it.advance();
				long key = it.key();
				node.addFloat(graph, key, kBeta, calc.calc(depth) * it.value());
			}
			return null;
		}
	}

	private Dir dir;
	private int max;
	private Traversal tr;
	private String k;
	private TLongHashSet filters;
	private boolean returnVertices;
	private Beta calc;
	private String kBeta;

	public GraphCountStep(Beta calc, Dir dir, TLongHashSet vertices,
			int max_edges, Traversal tr, String k, boolean returnVertices,
			String kBeta) {
		this.calc = calc;
		this.filters = vertices;
		this.dir = dir;
		this.max = max_edges;
		this.tr = tr;
		this.k = k;
		this.kBeta = kBeta;
		this.returnVertices = returnVertices;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {

		Integer depth = (Integer) tr.get("current_depth");
		if (depth == null) {
			depth = 1;
		}

		tr.set("current_depth", depth + 1);

		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv()
				.getClientEnv(jobClient.getLocalPeer());

		TLongHashSet vertices = before.vertices();

		log.info("Graph count for " + vertices.size());

		final List<Pair<Node, TLongArrayList>> mapped = map.map(1,
				vertices.toArray(), ctx);

		ForkJoinTask<long[]> fj = new ForkJoinTask<>();

		for (Pair<Node, TLongArrayList> e : mapped) {
			fj.putJob(
					new GraphCount(filters, tr.getGraph().getGraph(), k, dir,
							max, e.getValue().toArray(), returnVertices),
					e.getKey());
		}

		TLongHashSet res = fj.execute(CountStep.JOBS,
				new ResultListener<long[], TLongHashSet>() {
					TLongHashSet temp = new TLongHashSet();

					@Override
					public void onSuccess(long[] sr) {
						log.info("Received count set of size " + sr.length);
						if (sr.length != 0)
							synchronized (temp) {
								temp.addAll(sr);
							}
					}

					@Override
					public TLongHashSet onFinished() {
						log.info("Finished count task of " + temp.size());
						return temp;
					}

					@Override
					public void onFailure(Exception res) {
					}
				});
		log.info("Persisting temporal floats: " + k);

		tr.getGraph().commitFloatUpdates(k);

		if (calc != null && calc.mustSave(depth)) {
			log.info("Persisting beta counts: " + kBeta);
			tr.getGraph().gather(new BetaGather(depth, k, kBeta, calc));
			log.info("Finished Persisting beta counts: " + kBeta);
		}
		return new GraphCountResult(res, tr.getGraph(), k);
	}

}
