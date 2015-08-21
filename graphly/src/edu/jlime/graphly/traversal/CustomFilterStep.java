package edu.jlime.graphly.traversal;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.jobs.MapperFactory;
import edu.jlime.graphly.rec.VertexFilter;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class CustomFilterStep implements Step {

	private VertexFilter f;
	private GraphlyTraversal tr;

	public CustomFilterStep(VertexFilter f, GraphlyTraversal tr) {
		this.f = f;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(
				GraphlyClient.NUM_JOBS, before.vertices().toArray(), ctx);

		ForkJoinTask<TLongHashSet> fj = new ForkJoinTask<>();

		if (mapped.size() == 1) {
			fj.putJob(new FilterJob(tr.getGraph(), f, before.vertices()
					.toArray()), mapped.get(0).left);
		} else {
			for (Pair<ClientNode, TLongArrayList> e : mapped) {
				fj.putJob(new FilterJob(tr.getGraph(), f, e.getValue()
						.toArray()), e.getKey());
			}
		}

		TLongHashSet ret = fj.execute(16,
				new ResultListener<TLongHashSet, TLongHashSet>() {
					final TLongHashSet temp = new TLongHashSet();

					AtomicInteger jobCount = new AtomicInteger(mapped.size());

					@Override
					public void onSuccess(TLongHashSet subres) {
						synchronized (temp) {
							log.info("Received filter result, remaining "
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
