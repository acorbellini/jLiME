package edu.jlime.graphly.rec;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class ExploratoryCountStep implements CustomFunction {

	private int max;
	private Dir[] dirs;

	public ExploratoryCountStep(int max_edges, Dir[] dirs) {
		this.max = max_edges;
		this.dirs = dirs;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		TLongHashSet vertices = before.vertices();

		for (Dir dir : dirs) {
			final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(1,
					vertices.toArray(), ctx);

			ForkJoinTask<TLongHashSet> fj = new ForkJoinTask<>();

			if (mapped.size() == 1) {
				fj.putJob(new GraphCount(tr.getGraph(), "count", dir, max,
						vertices.toArray()), mapped.get(0).left);
			} else {
				for (Pair<ClientNode, TLongArrayList> e : mapped) {
					fj.putJob(new GraphCount(tr.getGraph(), "count", dir, max,
							e.getValue().toArray()), e.getKey());
				}
			}

			vertices = fj.execute(CountStep.JOBS,
					new ResultListener<TLongHashSet, TLongHashSet>() {
						TLongHashSet temp = new TLongHashSet();

						@Override
						public void onSuccess(TLongHashSet sr) {
							log.info("Received count set.");
							if (!sr.isEmpty())
								synchronized (temp) {
									temp.addAll(sr);
								}
						}

						@Override
						public TLongHashSet onFinished() {
							log.info("Finished count task.");
							return temp;
						}

						@Override
						public void onFailure(Exception res) {
						}
					});

			tr.getGraph().commitFloatUpdates("ec");
		}

		return before;
	}

}
