package edu.jlime.graphly.rec;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.traversal.ValueResult;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

public class AdamicAdar implements CustomFunction {

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(
				GraphlyClient.NUM_JOBS, before.vertices().toArray(), ctx);

		ForkJoinTask<Float> fj = new ForkJoinTask<>();

		for (Pair<ClientNode, TLongArrayList> e : mapped) {
			fj.putJob(new AdamicJob(tr.getGraph(), e.getValue()), e.getKey());
		}

		Float finalRes = fj.execute(16, new ResultListener<Float, Float>() {
			float temp = 0f;

			AtomicInteger jobCount = new AtomicInteger(mapped.size());

			@Override
			public synchronized void onSuccess(Float subres) {
				log.info("Received result, remaining "
						+ jobCount.decrementAndGet() + " jobs.");
				temp += subres;
			}

			@Override
			public Float onFinished() {
				return temp;
			}

			@Override
			public void onFailure(Exception res) {
			}
		});
		return new ValueResult(finalRes);
	}

}
