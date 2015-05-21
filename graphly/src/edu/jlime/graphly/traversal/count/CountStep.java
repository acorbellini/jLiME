package edu.jlime.graphly.traversal.count;

import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.util.Pair;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;

public class CountStep implements Step {

	private Dir dir;
	private GraphlyTraversal tr;
	private int top;
	private int max_edges;

	public CountStep(Dir dir, int top, int max_edges, GraphlyTraversal gt) {
		this.dir = dir;
		this.tr = gt;
		this.top = top;
		this.max_edges = max_edges;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		JobDispatcher jobClient = tr.getGraph().getJobClient();

		JobContextImpl ctx = jobClient.getEnv().getClientEnv(
				jobClient.getLocalPeer());

		final List<Pair<ClientNode, TLongArrayList>> mapped = map.map(
				Graphly.NUM_JOBS, before.vertices().toArray(), ctx);

		ForkJoinTask<GraphlyCount> fj = new ForkJoinTask<>();

		for (Pair<ClientNode, TLongArrayList> e : mapped) {
			fj.putJob(new CountJob(tr.getGraph(), dir, max_edges, e.getValue()
					.toArray()), e.getKey());
		}
		// DB db = DBMaker.newTempFileDB().mmapFileEnable().cacheLRUEnable()
		// .cacheSize(100 * 1024 * 1024).make();
		// final HTreeMap<Long, Integer> count = db.createHashMap("count")
		// .keySerializer(Serializer.LONG)
		// .valueSerializer(Serializer.INTEGER).make();
		// final NavigableSet<Tuple2<Integer, Long>> second = db.createTreeSet(
		// "count-set").make();

		TLongFloatHashMap finalRes = fj.execute(16,
				new ResultListener<GraphlyCount, TLongFloatHashMap>() {
					final TLongFloatHashMap temp = new TLongFloatHashMap();

					AtomicInteger jobCount = new AtomicInteger(mapped.size());

					@Override
					public void onSuccess(GraphlyCount subres) {
						synchronized (temp) {
							// if (log.isDebugEnabled())
							log.info("Received result, remaining "
									+ jobCount.decrementAndGet());
							TLongIntIterator it = subres.iterator();
							while (it.hasNext()) {
								it.advance();
								// Integer c = count.get(it.key());
								// if (c == null)
								// c = 0;
								// int newCount = c + it.value();
								// count.put(it.key(), newCount);
								// second.remove(new Tuple2<>(it.value(),
								// it.key()));
								// second.add(new Tuple2<>(newCount, it.key()));
								temp.adjustOrPutValue(it.key(), it.value(),
										it.value());
							}
							if (log.isDebugEnabled())
								log.debug("Finished adding to result.");
						}
					}

					@Override
					public TLongFloatHashMap onFinished() {
						TreeSet<Long> finalRes = new TreeSet<Long>(
								new Comparator<Long>() {

									@Override
									public int compare(Long o1, Long o2) {
										int comp = Float.compare(temp.get(o1),
												temp.get(o2));
										if (comp == 0)
											return o1.compareTo(o2);
										return comp;
									}
								});
						TLongFloatIterator it = temp.iterator();
						while (it.hasNext()) {
							it.advance();
							long k = it.key();
							float v = it.value();
							if (finalRes.size() < top)
								finalRes.add(k);
							else {
								if (v > temp.get(finalRes.first())) {
									finalRes.remove(finalRes.first());
									finalRes.add(k);
								}
							}
						}
						TLongFloatHashMap res = new TLongFloatHashMap();
						for (Long k : finalRes) {
							res.put(k, temp.get(k));
						}
						return res;
					}

					@Override
					public void onFailure(Exception res) {
					}
				});
		// db.close();
		return new CountResult(finalRes);
	}

	@Override
	public String toString() {
		return "CountStep [dir=" + dir + ", top=" + top + ", max_edges="
				+ max_edges + "]";
	}

}
