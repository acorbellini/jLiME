package edu.jlime.graphly.traversal;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.graphly.util.Pair;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.job.RunJob;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import gnu.trove.list.array.TLongArrayList;

public class RepeatStep implements Step {
	private static class RepeatJob extends RunJob {

		private Repeat<long[]> func;
		private long[] value;

		public RepeatJob(Repeat<long[]> rfunc, long[] ls) {
			this.func = rfunc;
			this.value = ls;
		}

		@Override
		public void run(JobContext env, ClientNode origin) throws Exception {
			// Logger log = Logger.getLogger(RepeatJob.class);
			// if (log.isDebugEnabled())
			// log.debug("Executing Repeat job");
			func.exec(value, (Graphly) env.getGlobal("graphly"));
		}
	}

	private int steps;
	private Repeat<long[]> rfunc;
	private GraphlyTraversal tr;

	public RepeatStep(int steps, Repeat<long[]> rfunc, GraphlyTraversal tr) {
		this.steps = steps;
		this.rfunc = rfunc;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		Logger log = Logger.getLogger(RepeatStep.class);
		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		long[] array = before.vertices().toArray();

		for (int i = 0; i < steps; i++) {
			final List<Pair<ClientNode, TLongArrayList>> div = map.map(
					Graphly.NUM_JOBS, array, ctx);
			if (log.isDebugEnabled()) {
				log.debug("Current Repeat Step: " + i + "/" + steps);
				log.debug("Executing " + div.size() + " jobs.");
			}
			ForkJoinTask<Boolean> fj = new ForkJoinTask<>();
			for (Pair<ClientNode, TLongArrayList> e : div) {
				fj.putJob(new RepeatJob(rfunc, e.getValue().toArray()),
						e.getKey());
			}

			fj.execute(new ResultListener<Boolean, Boolean>() {
				AtomicInteger cont = new AtomicInteger(div.size());

				@Override
				public void onSuccess(Boolean result) {
					// System.out.println("Completed job, remaining "
					// + cont.decrementAndGet());
				}

				@Override
				public Boolean onFinished() {
					return null;
				}

				@Override
				public void onFailure(Exception res) {
					res.printStackTrace();
				}
			});
		}
		return before;
	}
}
