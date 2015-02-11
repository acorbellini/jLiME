package edu.jlime.graphly.traversal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.traversal.recommendation.ForEach;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.list.array.TLongArrayList;

public class RepeatStep<T> implements Step<Long, List<T>> {

	public static class Rjob<T> implements Job<List<T>> {

		private long vid;
		private ForEach<T> fe;
		private int steps;

		public Rjob(int steps, long vid, ForEach<T> fe) {
			this.vid = vid;
			this.fe = fe;
			this.steps = steps;
		}

		@Override
		public List<T> call(JobContext env, ClientNode peer) throws Exception {
			List<T> res = new ArrayList<>();
			for (int i = 0; i < steps; i++) {
				Graphly g = (Graphly) env.getGlobal("graphly");
				res.add(fe.exec(vid, g));
			}
			return res;
		}

	}

	private ForEach<T> forEach;
	private int s;
	private GraphlyTraversal tr;

	public RepeatStep(int steps, ForEach<T> forEach, GraphlyTraversal tr) {
		this.s = steps;
		this.forEach = forEach;
		this.tr = tr;
	}

	@Override
	public List<T> exec(Long before) throws Exception {
		final Mapper map = (Mapper) tr.get("mapper");
		ClientNode local = tr.getGraph().getJobClient().getCluster()
				.getLocalNode();

		return local.exec(new Job<List<T>>() {
			@Override
			public List<T> call(JobContext ctx, ClientNode peer)
					throws Exception {
				Map<ClientNode, TLongArrayList> m = map.map(
						new long[] { before }, ctx);
				return m.keySet().iterator().next()
						.exec(new Rjob<T>(s, before, forEach));
			}
		});
	}
}
