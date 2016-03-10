package edu.jlime.graphly.traversal;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.VertexFilter;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class CustomFilterStep implements Step {

	private VertexFilter f;
	private Traversal tr;

	public CustomFilterStep(VertexFilter f, Traversal tr) {
		this.f = f;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		final List<Pair<Node, TLongArrayList>> mapped = map.map(Graphly.NUM_JOBS, before.vertices().toArray(), ctx);

		ForkJoinTask<long[]> fj = new ForkJoinTask<>();

		if (mapped.size() == 1) {
			fj.putJob(new FilterJob(tr.getGraph(), f, before.vertices().toArray()), mapped.get(0).left);
		} else {
			for (Pair<Node, TLongArrayList> e : mapped) {
				fj.putJob(new FilterJob(tr.getGraph(), f, e.getValue().toArray()), e.getKey());
			}
		}

		TLongHashSet ret = fj.execute(16, new ResultListener<long[], TLongHashSet>() {
			final TLongHashSet temp = new TLongHashSet();

			AtomicInteger jobCount = new AtomicInteger(mapped.size());

			@Override
			public void onSuccess(long[] subres) {
				synchronized (temp) {
					log.info("Received filter result of size " + subres.length + ", remaining "
							+ jobCount.decrementAndGet() + " jobs.");
					temp.addAll(subres);
				}
			}

			@Override
			public TLongHashSet onFinished() {
				return temp;
			}

			@Override
			public void onFailure(Exception res) {
			}
		});
		return new VertexResult(ret);
	}

}
