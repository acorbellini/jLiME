package edu.jlime.collections.adjacencygraph.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.mapreduce.MapReduceTask;
import gnu.trove.list.array.TIntArrayList;

public class RemoteForEachQuery<T> extends RemoteQuery<Map<Integer, T>> {

	private static final long serialVersionUID = -8863815088794815587L;

	public static class ForEachRes<T> implements Serializable {

		private static final long serialVersionUID = 1L;

		int u;

		T res;

		public ForEachRes(int u, T res) {
			super();
			this.u = u;
			this.res = res;
		}

		public int getU() {
			return u;
		}

		public T getRes() {
			return res;
		}

	}

	public static class ForEachJob<T> implements Job<Map<Integer, T>> {

		private static final long serialVersionUID = -6601170162685948088L;

		int[] usersid;

		private ForEachQueryProc<T> proc;

		private RemoteAdjacencyGraph graph;

		public ForEachJob(int[] users, ForEachQueryProc<T> proc,
				RemoteAdjacencyGraph graph) {
			this.usersid = users;
			this.proc = proc;
			this.graph = graph;
		}

		@Override
		public Map<Integer, T> call(final JobContext ctx, JobNode peer)
				throws Exception {
			final Logger log = Logger.getLogger(ForEachJob.class);
			if (log.isDebugEnabled())
				log.debug("Executing procedure for each of " + usersid.length
						+ " users.");
			final Map<Integer, T> subres = Collections
					.synchronizedMap(new HashMap<Integer, T>());
			ExecutorService exec = Executors.newCachedThreadPool();
			final Semaphore toAdd = new Semaphore(6);
			final Semaphore lock = new Semaphore(-usersid.length + 1);
			for (final int u : usersid) {
				toAdd.acquire();
				exec.execute(new Runnable() {

					@Override
					public void run() {
						try {
							subres.put(u, proc.call(new UserQuery(graph,
									new int[] { u })));
						} catch (Exception e) {
							log.error("", e);
						}
						toAdd.release();
						lock.release();
						if (lock.availablePermits() % 1000 == 0)
							if (log.isDebugEnabled())
								log.debug(lock.availablePermits()
										+ " users remaining.");

					}
				});
			}
			lock.acquire();

			return subres;
		}
	}

	public static class ForEachMR<T> extends
			MapReduceTask<int[], Map<Integer, T>, Map<Integer, T>> {

		private static final long serialVersionUID = 3338509874687671311L;

		Map<Integer, T> res = Collections
				.synchronizedMap(new HashMap<Integer, T>());

		private ForEachQueryProc<T> proc;

		private RemoteAdjacencyGraph graph;

		public ForEachMR(int[] data, ForEachQueryProc<T> proc,
				RemoteAdjacencyGraph graph) {
			super(data);
			super.setDontCacheSubResults(true);
			this.proc = proc;
			this.graph = graph;
		}

		@Override
		public Map<Job<?>, JobNode> map(int[] data, JobContext env)
				throws Exception {
			Map<Job<?>, JobNode> res = new HashMap<>();
			Map<JobNode, TIntArrayList> map = graph.getMapper().map(data, env);

			for (JobNode p : map.keySet()) {
				res.put(new ForEachJob<T>(map.get(p).toArray(), proc, graph), p);
			}

			return res;
		}

		@Override
		public void processSubResult(Map<Integer, T> subres) {
			res.putAll(subres);

		}

		@Override
		public Map<Integer, T> red(ArrayList<Map<Integer, T>> subres) {
			return res;
		}

	}

	private RemoteListQuery query;

	private ForEachQueryProc<T> proc;

	public RemoteForEachQuery(RemoteListQuery query, ForEachQueryProc<T> proc) {
		super(query.getGraph());
		super.setCacheQuery(false);
		this.query = query;
		this.proc = proc;
	}

	@Override
	public String getMapName() {
		return query.getMapName();
	}

	@Override
	public Mapper getMapper() {
		return query.getMapper();
	}

	@Override
	protected Map<Integer, T> doExec(JobContext c) throws Exception {
		return new ForEachMR<T>(query.exec(c), proc, getGraph()).exec(c
				.getCluster());

		// Map<Integer, T> map = new HashMap<>();
		//
		// for (int u : query.exec(c)) {
		//
		// map.put(u, proc.call(new UserQuery(new int[] { u }, getMapName(),
		// getMapper()), c.getCluster()));
		// }
		//
		// return map;

	}

	public static void main(String[] args) {
		// int MAX_USERS = 1;
		// int[] data = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
		// 14,
		// 15 };
		// float max = data.length / (float) MAX_USERS;
		// int parts = (int) Math.ceil(max);
		// for (int i = 0; i < parts; i++) {
		// if (i * MAX_USERS + MAX_USERS >= data.length)
		// System.out.println(Arrays.toString(ArrayUtils.subarray(data, i
		// * MAX_USERS, data.length)));
		// else {
		// System.out.println(Arrays.toString(ArrayUtils.subarray(data, i
		// * MAX_USERS, i * MAX_USERS + MAX_USERS)));
		// }
		// }
	}
}