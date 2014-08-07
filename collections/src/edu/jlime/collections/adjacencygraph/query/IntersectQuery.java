package edu.jlime.collections.adjacencygraph.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.jlime.client.JobContext;
import edu.jlime.collections.util.IntArrayUtils;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.mapreduce.MapReduceTask;

public class IntersectQuery extends RemoteListQuery {

	private static final long serialVersionUID = -8645285678430981820L;

	private RemoteListQuery left;

	private ListQuery right;

	public static class IntersectJob implements Job<int[]> {

		private static final long serialVersionUID = 6270129777456714628L;

		private RemoteListQuery q;

		public IntersectJob(RemoteListQuery query) {
			this.q = query;
		}

		@Override
		public int[] call(JobContext ctx, JobNode peer) throws Exception {
			return q.exec(ctx);
		}
	}

	public static class IntersectMR extends
			MapReduceTask<RemoteListQuery[], int[], int[]> {

		private static final long serialVersionUID = 1941553160824566482L;

		public IntersectMR(RemoteListQuery[] data) {
			super(data);
		}

		@Override
		public Map<Job<?>, JobNode> map(RemoteListQuery[] data, JobContext c)
				throws Exception {
			Map<Job<?>, JobNode> res = new HashMap<>();
			for (RemoteListQuery query : data) {
				res.put(new IntersectJob(query), c.getCluster()
						.getAnyExecutor());
			}
			return res;
		}

		@Override
		public int[] red(ArrayList<int[]> subres) {
			int[] res = null;
			for (int[] is : subres) {
				if (res == null)
					res = is;
				else
					res = IntArrayUtils.intersectArrays(res, is);
			}
			return res;
		}

	}

	public IntersectQuery(RemoteListQuery left, ListQuery listQuery) {
		super(left.getGraph());
		this.left = left;
		this.right = listQuery;
		setCacheQuery(false);
	}

	@Override
	protected int[] doExec(JobContext c) throws Exception {
		// return new IntersectMR().exec(new ListQuery[] { left, right },
		// Cluster.get());
		return IntArrayUtils.intersectArrays(left.exec(c),
				((RemoteListQuery) right).exec(c));
	}

}
