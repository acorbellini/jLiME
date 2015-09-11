package edu.jlime.graphly.traversal.each;

import java.util.List;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

public class EachStep<T> implements Step {

	private ForEach<T> forEach;
	private int s;
	private Traversal tr;
	private String key;

	public EachStep(String key, int steps, ForEach<T> forEach, Traversal tr) {
		this.key = key;
		this.s = steps;
		this.forEach = forEach;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		List<Pair<Node, TLongArrayList>> mapped = map.map(Graphly.NUM_JOBS, before.vertices().toArray(), ctx);

		ForkJoinTask<Boolean> fj = new ForkJoinTask<>();

		for (Pair<Node, TLongArrayList> e : mapped) {
			fj.putJob(new EachJob<T>(tr.getGraph(), s, key, e.getValue().toArray(), forEach), e.getKey());
		}

		fj.execute(new ResultListener<Boolean, Void>() {

			@Override
			public void onSuccess(Boolean result) {
			}

			@Override
			public Void onFinished() {
				return null;
			}

			@Override
			public void onFailure(Exception res) {
			}
		});
		return before;
	}
}
