package edu.jlime.collections.adjacencygraph.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.jlime.client.JobContext;
import edu.jlime.collections.util.IntArrayUtils;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.mapreduce.MapReduceTask;
import gnu.trove.set.hash.TIntHashSet;

public class UnionQuery extends RemoteListQuery {

	private static final long serialVersionUID = 4745290758931750933L;

	private RemoteListQuery left;

	private ListQuery right;

	public static class UnionJob implements Job<int[]> {

		private static final long serialVersionUID = -8018630034656694837L;

		private RemoteListQuery q;

		public UnionJob(RemoteListQuery query) {
			this.q = query;
		}

		@Override
		public int[] call(JobContext ctx, JobNode peer) throws Exception {
			return q.exec(ctx);
		}

	}

	public static class UnionMR extends
			MapReduceTask<RemoteListQuery[], int[], int[]> {

		private static final long serialVersionUID = 2702363430980170098L;

		public UnionMR(RemoteListQuery[] data) {
			super(data);
		}

		@Override
		public Map<Job<?>, JobNode> map(RemoteListQuery[] data, JobContext env)
				throws Exception {
			Map<Job<?>, JobNode> res = new HashMap<>();
			for (RemoteListQuery query : data) {
				res.put(new UnionJob(query), env.getCluster().getAnyExecutor());
			}
			return res;
		}

		@Override
		public int[] red(ArrayList<int[]> subres) {
			TIntHashSet res = new TIntHashSet();
			for (int[] is : subres) {
				res.addAll(is);
			}
			return res.toArray();
		}

	}

	public UnionQuery(RemoteListQuery left, ListQuery query) {
		super(left.getGraph());
		this.left = left;
		this.right = query;
		setCacheQuery(false);
	}

	@Override
	protected int[] doExec(JobContext c) throws Exception {
		return IntArrayUtils.union(left.exec(c),
				((RemoteListQuery) right).exec(c));
		// return new UnionMR().exec(new ListQuery[] { left, right },
		// Cluster.get());
	}
}
