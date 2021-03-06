package edu.jlime.graphly.rec;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.traversal.ValueResult;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class JaccardStep implements CustomFunction {

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		final Logger log = Logger.getLogger(CountStep.class);

		Mapper map = (Mapper) tr.get("mapper");

		Dispatcher jobClient = tr.getGraph().getJobClient();

		JobContext ctx = jobClient.getEnv().getClientEnv(jobClient.getLocalPeer());

		final List<Pair<Node, TLongArrayList>> mapped = map.map(Graphly.NUM_JOBS, before.vertices().toArray(), ctx);

		ForkJoinTask<JaccardSubResult> fj = new ForkJoinTask<>();

		for (Pair<Node, TLongArrayList> e : mapped) {
			fj.putJob(new JaccardJob(tr.getGraph(), e.getValue()), e.getKey());
		}

		Float finalRes = fj.execute(16, new ResultListener<JaccardSubResult, Float>() {
			JaccardSubResult temp = new JaccardSubResult();

			AtomicInteger jobCount = new AtomicInteger(mapped.size());

			@Override
			public synchronized void onSuccess(JaccardSubResult subres) {
				log.info("Received result, remaining " + jobCount.decrementAndGet() + " jobs.");
				if (temp.intersect == null)
					temp.intersect = subres.intersect;
				else {
					TLongIterator it = temp.intersect.iterator();
					while (it.hasNext()) {
						if (!subres.intersect.contains(it.next()))
							it.remove();
					}
				}
				if (temp.union == null)
					temp.union = subres.union;
				else {
					temp.union.addAll(subres.union);
				}
			}

			@Override
			public Float onFinished() {
				return temp.intersect.size() / (float) temp.union.size();
			}

			@Override
			public void onFailure(Exception res) {
			}
		});
		return new ValueResult(finalRes);
	}

}
