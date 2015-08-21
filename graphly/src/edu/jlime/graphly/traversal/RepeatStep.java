package edu.jlime.graphly.traversal;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.job.RunJob;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

public class RepeatStep implements Step {
	private static class RepeatJob extends RunJob {

		private Repeat<long[]> func;
		private long[] value;
		private GraphlyGraph g;

		public RepeatJob(GraphlyGraph g, Repeat<long[]> rfunc, long[] ls) {
			this.func = rfunc;
			this.value = ls;
			this.g = g;
		}

		@Override
		public void run(JobContext env, ClientNode origin) throws Exception {
			// Logger log = Logger.getLogger(RepeatJob.class);
			// if (log.isDebugEnabled())
			// log.debug("Executing Repeat job");
			func.exec(value, g);
		}
	}

	public interface RepeatSync<T> {
		public void exec(T before, GraphlyGraph g) throws Exception;
	}

	private int steps;
	private Repeat<long[]> rfunc;
	private GraphlyTraversal tr;
	private RepeatSync<long[]> sync;

	public RepeatStep(int steps, Repeat<long[]> rfunc, RepeatSync<long[]> sync,
			GraphlyTraversal tr) {
		this.steps = steps;
		this.rfunc = rfunc;
		this.tr = tr;
		this.sync = sync;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(RepeatStep.class);
		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		long[] array = before.vertices().toArray();

		List<Pair<ClientNode, TLongArrayList>> div = map.map(GraphlyClient.NUM_JOBS,
				array, ctx);
		if (!div.isEmpty())
			for (int i = 0; i < steps; i++) {
				if (map.isDynamic())
					div = map.map(GraphlyClient.NUM_JOBS, array, ctx);
				final int divSize = div.size();
				log.info("Current Repeat Step: " + i + "/" + steps
						+ ". Executing " + div.size() + " jobs.");
				ForkJoinTask<Boolean> fj = new ForkJoinTask<>();
				for (Pair<ClientNode, TLongArrayList> e : div) {
					fj.putJob(new RepeatJob(tr.getGraph(), rfunc, e.getValue()
							.toArray()), e.getKey());
				}

				fj.execute(new ResultListener<Boolean, Boolean>() {
					AtomicInteger cont = new AtomicInteger(divSize);

					@Override
					public void onSuccess(Boolean result) {
						if (log.isDebugEnabled())
							log.debug("Completed job, remaining "
									+ cont.decrementAndGet());
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

				sync.exec(array, tr.getGraph());
			}
		return before;
	}
}
