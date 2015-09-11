package edu.jlime.graphly.traversal;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class IntersectStep implements Step {

	private Dir dir;
	private Traversal tr;

	public IntersectStep(Dir dir, Traversal graphlyTraversal) {
		this.dir = dir;
		this.tr = graphlyTraversal;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		final List<Pair<Node, TLongArrayList>> mapped = map.map(Graphly.NUM_JOBS, before.vertices().toArray(), ctx);

		ForkJoinTask<TLongHashSet> fj = new ForkJoinTask<>();

		for (Pair<Node, TLongArrayList> e : mapped) {
			fj.putJob(new IntersectJob(tr.getGraph(), dir, e.getValue()), e.getKey());
		}

		TLongHashSet finalRes = fj.execute(16, new ResultListener<TLongHashSet, TLongHashSet>() {
			TLongHashSet temp;

			AtomicInteger jobCount = new AtomicInteger(mapped.size());

			@Override
			public synchronized void onSuccess(TLongHashSet subres) {
				log.info("Received result, remaining " + jobCount.decrementAndGet() + " jobs.");
				if (temp == null)
					temp = subres;
				else {
					TLongIterator it = temp.iterator();
					while (it.hasNext()) {
						if (!subres.contains(it.next()))
							it.remove();
					}
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
		return new VertexResult(finalRes);
	}
}
