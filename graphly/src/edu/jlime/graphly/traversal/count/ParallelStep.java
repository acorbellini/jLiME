package edu.jlime.graphly.traversal.count;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class ParallelStep implements Step {
	private GraphlyTraversal tr;
	private Step task;
	private int div;
	private ResultListener<TraversalResult, TraversalResult> merger;

	public ParallelStep(Step step, GraphlyTraversal gt, int div,
			ResultListener<TraversalResult, TraversalResult> merger) {
		this.tr = gt;
		this.task = step;
		this.div = div;
		this.merger = merger;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(ParallelStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(1, before
				.vertices().toArray(), ctx);

		ForkJoinTask<TraversalResult> fj = new ForkJoinTask<>();

		TLongHashSet[] tasks = new TLongHashSet[div];
		ClientNode[] c = new ClientNode[div];
		int cont = 0;
		for (Pair<ClientNode, TLongArrayList> e : mapped) {
			TLongHashSet l = tasks[cont % div];
			if (l == null) {
				l = new TLongHashSet();
				tasks[cont % div] = l;
			}

			if (c[cont % div] == null)
				c[cont % div] = e.getKey();

			l.addAll(e.getValue());
			cont++;
		}
		for (int i = 0; i < tasks.length; i++) {
			fj.putJob(new ParallelStepJob(tasks[i], task, tr), c[i]);
		}

		return fj.execute(Integer.MAX_VALUE, merger);
	}
}
