package edu.jlime.graphly.traversal;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.GraphCount;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class GraphCountStep implements Step {

	private Dir dir;
	private int max;
	private GraphlyTraversal tr;
	private String k;
	private String[] filters;
	private boolean returnVertices;

	public GraphCountStep(Dir dir, String[] filters, int max_edges,
			GraphlyTraversal tr, String k, boolean returnVertices) {
		this.filters = filters;
		this.dir = dir;
		this.max = max_edges;
		this.tr = tr;
		this.k = k;
		this.returnVertices = returnVertices;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		TLongHashSet vertices = before.vertices();

		log.info("Graph count for " + vertices.size());

		final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(1,
				vertices.toArray(), ctx);

		ForkJoinTask<long[]> fj = new ForkJoinTask<>();

		for (Pair<ClientNode, TLongArrayList> e : mapped) {
			fj.putJob(new GraphCount(filters, tr.getGraph(), k, dir, max, e
					.getValue().toArray(), returnVertices), e.getKey());
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

		tr.getGraph().commitFloatUpdates(k);

		return new GraphCountResult(res, tr.getGraph(), k);
	}
}
