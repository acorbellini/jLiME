package edu.jlime.graphly.traversal;

import java.util.List;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.util.Pair;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class VertexStep implements Step {

	private Dir dir;
	private GraphlyTraversal tr;
	private int max_edges;

	public VertexStep(Dir dir, int max_edges, GraphlyTraversal tr) {
		this.dir = dir;
		this.tr = tr;
		this.max_edges = max_edges;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		List<Pair<ClientNode, TLongArrayList>> div = map.map(
				Graphly.MAX_IDS_PER_JOB, before.vertices().toArray(), ctx);
		ForkJoinTask<long[]> fj = new ForkJoinTask<>();
		for (Pair<ClientNode, TLongArrayList> e : div) {
			fj.putJob(new VertexJob(dir, max_edges, e.getValue().toArray()),
					e.getKey());
		}
		long[] finalRes = fj.execute(new ResultListener<long[], long[]>() {
			TLongHashSet ret = new TLongHashSet();

			@Override
			public synchronized void onSuccess(long[] result) {
				ret.addAll(result);
			}

			@Override
			public long[] onFinished() {
				return ret.toArray();
			}

			@Override
			public void onFailure(Exception res) {
			}
		});
		return new VertexResult(new TLongHashSet(finalRes));
	}
}
