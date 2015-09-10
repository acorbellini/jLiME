package edu.jlime.graphly.rec;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.traversal.count.CountJob;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;

public class BetaCountStep implements CustomFunction {

	// private float beta;
	private int depth;
	private BetaCalc beta;
	private Dir dir;

	public BetaCountStep(BetaCalc beta, int depth, Dir dir) {
		this.beta = beta;
		this.depth = depth;
		this.dir = dir;
	}

	@Override
	public TraversalResult execute(TraversalResult before,
			final GraphlyTraversal tr) throws Exception {
		final TLongFloatMap adj = before.getCounts();
		final TLongFloatHashMap res = new TLongFloatHashMap();

		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		long initCount = System.currentTimeMillis();

		for (int j = 0; j < depth; j++) {
			final int current_depth = j;

			log.info("Mapping " + adj.keys().length + " vertices.");

			final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(
					GraphlyClient.NUM_JOBS, adj.keys(), ctx);

			final ForkJoinTask<GraphlyCount> fj = new ForkJoinTask<>();

			if (mapped.size() == 1) {
				fj.putJob(new CountJob(tr.getGraph(), dir, Integer.MAX_VALUE,
						adj.keys(), adj.values(), null), mapped.get(0).left);
			} else {
				ExecutorService exec = Executors.newFixedThreadPool(Runtime
						.getRuntime().availableProcessors());

				for (final Pair<ClientNode, TLongArrayList> e : mapped) {
					exec.execute(new Runnable() {
						@Override
						public void run() {
							try {
								TLongFloatHashMap prevCounts = new TLongFloatHashMap();
								TLongArrayList value = e.getValue();
								TLongIterator it = value.iterator();
								while (it.hasNext()) {
									long v = it.next();
									prevCounts.put(v, adj.get(v));
								}
								log.info("Creating job of " + prevCounts.size()
										+ " vertices with counts.");
								synchronized (fj) {
									fj.putJob(
											new CountJob(tr.getGraph(), dir,
													Integer.MAX_VALUE,
													prevCounts.keys(),
													prevCounts.values(), null),
											e.getKey());
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					});
				}
				exec.shutdown();
				exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			}
			long initStep = System.currentTimeMillis();
			fj.execute(CountStep.JOBS,
					new ResultListener<GraphlyCount, Void>() {

						@Override
						public void onSuccess(GraphlyCount graphlyCount) {
							log.info("Received result of "
									+ graphlyCount.size());
							long init = System.currentTimeMillis();
							float currbeta = beta.calc(current_depth + 1);

							TLongFloatIterator it = graphlyCount.iterator();
							synchronized (res) {
								while (it.hasNext()) {
									it.advance();
									long key = it.key();
									float value = it.value();
									if (current_depth != depth - 1)
										adj.adjustOrPutValue(key, value, value);

									if (beta.mustSave(current_depth + 1)) {
										float currentVal = currbeta * value;
										res.adjustOrPutValue(key, currentVal,
												currentVal);
									}
								}

							}
							log.info("Finished adding to result in "
									+ (System.currentTimeMillis() - init)
									+ " ms.");
						}

						@Override
						public Void onFinished() {
							return null;
						}

						@Override
						public void onFailure(Exception res) {
						}
					});

			log.info("Finished beta count STEP " + current_depth + " in "
					+ (System.currentTimeMillis() - initStep) + " ms.");
		}

		log.info("Finished beta count in "
				+ (System.currentTimeMillis() - initCount) + " ms.");
		return new CountResult(res);
	}
}
