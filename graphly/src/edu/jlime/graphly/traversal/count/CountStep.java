package edu.jlime.graphly.traversal.count;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.RemoteReference;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class CountStep implements Step {

	public static final int JOBS = 64;

	private Dir dir;
	private GraphlyTraversal tr;
	private int max_edges;

	public CountStep(Dir dir, int max_edges, GraphlyTraversal gt) {
		this.dir = dir;
		this.tr = gt;
		this.max_edges = max_edges;
	}

	@Override
	public TraversalResult exec(final TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(
				2, before.vertices().toArray(), ctx);

		ForkJoinTask<RemoteReference<GraphlyCount>> fj = new ForkJoinTask<>();

		if (mapped.size() == 1) {
			fj.putJob(
					new CountJob(tr.getGraph(), dir, max_edges, before
							.getCounts()), mapped.get(0).left);
		} else {
			for (Pair<ClientNode, TLongArrayList> e : mapped) {
				TLongArrayList value = e.getValue();
				TLongIterator it = value.iterator();
				TLongFloatHashMap prevCounts = new TLongFloatHashMap();
				while (it.hasNext()) {
					long v = it.next();
					prevCounts.put(v, before.getCount(v));
				}
				fj.putJob(new CountJob(tr.getGraph(), dir, max_edges,
						prevCounts), e.getKey());
			}
		}

		TLongFloatHashMap finalRes = fj
				.execute(
						JOBS,
						new ResultListener<RemoteReference<GraphlyCount>, TLongFloatHashMap>() {
							final TLongFloatHashMap temp = new TLongFloatHashMap();

							@Override
							public void onSuccess(
									RemoteReference<GraphlyCount> sr) {
								GraphlyCount gc = null;
								try {
									gc = sr.get();
								} catch (Exception e) {
									e.printStackTrace();
								}

								log.info("Received result with "
										+ gc.getRes().size() + " vertices.");

								synchronized (temp) {
									TLongFloatIterator it = gc.getRes()
											.iterator();

									while (it.hasNext()) {
										it.advance();
										long key = it.key();
										float value = it.value();
										temp.adjustOrPutValue(key, value, value);
									}
								}
								if (log.isDebugEnabled())
									log.debug("Finished adding to result.");

							}

							@Override
							public TLongFloatHashMap onFinished() {
								log.info("Finished count task.");
								return temp;
							}

							@Override
							public void onFailure(Exception res) {
							}
						});
		return new CountResult(finalRes);
	}

	@Override
	public String toString() {
		return "CountStep [dir=" + dir + ", max_edges=" + max_edges + "]";
	}

}
