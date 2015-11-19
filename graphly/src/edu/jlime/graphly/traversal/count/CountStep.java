package edu.jlime.graphly.traversal.count;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class CountStep implements Step {

	public static final int JOBS = 64;

	private Dir dir;
	private Traversal tr;
	private int max_edges;

	private String[] filters;

	public CountStep(Dir dir, int max_edges, String[] filters, Traversal gt) {
		this.dir = dir;
		this.tr = gt;
		this.max_edges = max_edges;
		this.filters = filters;
	}

	@Override
	public TraversalResult exec(final TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		long init = System.currentTimeMillis();

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		final List<Pair<Node, TLongArrayList>> mapped = map.map(1, before.vertices().toArray(), ctx);

		final ForkJoinTask<Count> fj = new ForkJoinTask<>();

		final TLongHashSet toFilter = new TLongHashSet();
		for (String k : filters)
			toFilter.addAll(((TraversalResult) tr.get(k)).vertices());

		log.info("Creating Jobs.");

		final long[] filter = toFilter.toArray();
		if (mapped.size() == 1) {
			TLongFloatMap counts = before.getCounts();
			fj.putJob(new CountJob(tr.getGraph(), dir, max_edges, counts.keys(), counts.values(), filter),
					mapped.get(0).left);
		} else {
			ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

			for (final Pair<Node, TLongArrayList> e : mapped) {
				exec.execute(new Runnable() {
					@Override
					public void run() {
						try {
							TLongFloatHashMap prevCounts = new TLongFloatHashMap(100000);

							TLongArrayList value = e.getValue();
							for (int i = 0; i < value.size(); i++) {
								long v = value.get(i);
								prevCounts.put(v, before.getCount(v));
							}

							fj.putJob(new CountJob(tr.getGraph(), dir, max_edges, prevCounts.keys(),
									prevCounts.values(), filter), e.getKey());

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});

			}
			exec.shutdown();
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		}
		log.info("Finished creating jobs in " + (System.currentTimeMillis() - init));
		init = System.currentTimeMillis();
		TLongFloatMap finalRes = fj.execute(JOBS, new ResultListener<Count, TLongFloatMap>() {
			TLongFloatMap temp = null;

			@Override
			public void onSuccess(Count gc) {

				log.info("Received result with " + gc.size() + " vertices.");
				long init = System.currentTimeMillis();
				synchronized (this) {
					if (temp == null)
						temp = new TLongFloatHashMap(gc.keys(), gc.values());
					else {
						TLongFloatIterator it = gc.iterator();

						while (it.hasNext()) {
							it.advance();
							long key = it.key();
							float value = it.value();
							temp.adjustOrPutValue(key, value, value);
						}
					}
				}
				log.info("Finished adding to result in " + (System.currentTimeMillis() - init));

			}

			@Override
			public TLongFloatMap onFinished() {
				log.info("Finished count task.");
				return temp;
			}

			@Override
			public void onFailure(Exception res) {
			}
		});

		log.info("Finished Count Step in " + (System.currentTimeMillis() - init) + " ms");
		return new CountResult(finalRes);
	}

	@Override
	public String toString() {
		return "CountStep [dir=" + dir + ", max_edges=" + max_edges + "]";
	}

}
