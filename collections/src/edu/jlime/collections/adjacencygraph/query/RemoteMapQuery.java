package edu.jlime.collections.adjacencygraph.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.mapreduce.MapReduceTask;
import gnu.trove.list.array.TIntArrayList;

public class RemoteMapQuery<T> extends CompositeQuery<int[], Map<Integer, T>>
		implements MapQuery<T> {

	private static final long serialVersionUID = 1990654747426020860L;

	private MapProc<T> proc;

	public RemoteMapQuery(MapProc<T> mq, RemoteListQuery listQuery) {
		super(listQuery);
		this.proc = mq;
		super.setCacheQuery(false);
	}

	public static class MapJob<T> implements Job<Map<Integer, T>> {

		private static final long serialVersionUID = -5434814354860469938L;

		int[] usersid;

		private MapProc<T> proc;

		private RemoteAdjacencyGraph graph;

		public MapJob(int[] users, MapProc<T> proc, RemoteAdjacencyGraph graph) {
			this.usersid = users;
			this.proc = proc;
			this.graph = graph;
		}

		@Override
		public Map<Integer, T> call(JobContext ctx, ClientNode peer)
				throws Exception {
			Logger.getLogger(MapJob.class).info("Calling map procedure.");
			return proc.process(new UserQuery(graph, usersid));
		}
	}

	public static class MapMR<T> extends
			MapReduceTask<int[], Map<Integer, T>, Map<Integer, T>> {

		private static final long serialVersionUID = -8006850252346636453L;

		Map<Integer, T> result = Collections
				.synchronizedMap(new HashMap<Integer, T>());

		private MapProc<T> proc;

		private String mapName;

		private Mapper mapper;

		private RemoteAdjacencyGraph graph;

		public MapMR(int[] data, MapProc<T> proc, RemoteAdjacencyGraph graph) {
			super(data);
			super.setDontCacheSubResults(true);
			this.proc = proc;
			this.graph = graph;
		}

		@Override
		public Map<Job<Map<Integer, T>>, ClientNode> map(int[] data, JobContext env)
				throws Exception {
			Map<Job<Map<Integer, T>>, ClientNode> res = new HashMap<>();
			Map<ClientNode, TIntArrayList> map = mapper.map(data, env);
			Logger log = Logger.getLogger(MapMR.class);
			log.info("Mapping " + map.size() + " groups .");
			for (ClientNode p : map.keySet()) {
				if (log.isDebugEnabled())
					log.debug("Sending " + map.get(p).size() + " elements to "
							+ p);
				res.put(new MapJob<T>(map.get(p).toArray(), proc, graph), p);
			}

			return res;
		}

		@Override
		public void processSubResult(Map<Integer, T> subres) {
			Logger log = Logger.getLogger(MapMR.class);
			log.info("Reducing result at MapMR");
			result.putAll(subres);

		}

		@Override
		public Map<Integer, T> red(ArrayList<Map<Integer, T>> subres) {
			Logger log = Logger.getLogger(MapMR.class);
			log.info("Returning reduced result at MapMR");
			return result;
		}

	}

	@Override
	protected Map<Integer, T> doExec(JobContext c) throws Exception {

		// HashMap<Integer, T> res = new HashMap<>();
		// for (Peer p : map.keySet()) {
		// res.putAll(p.exec(new MapJob<T>(map.get(p).toArray(), proc,
		// getMapName(), getMapper())));
		// }

		int maxUsers = 10000;
		HashMap<Integer, T> res = new HashMap<>();
		int[] data = getQuery().exec(c);
		float max = data.length / (float) maxUsers;
		int parts = (int) Math.ceil(max);
		for (int i = 0; i < parts; i++) {
			int[] curr;
			Logger.getLogger(RemoteMapQuery.class).info(
					"Processing part " + (i + 1) + " of " + parts + " of "
							+ maxUsers + ".");
			if (i * maxUsers + maxUsers >= data.length)
				curr = Arrays.copyOfRange(data, i * maxUsers, data.length);
			else {
				curr = Arrays.copyOfRange(data, i * maxUsers, i * maxUsers
						+ maxUsers);
			}
			res.putAll(new MapMR<T>(curr, proc, getGraph()).exec(c.getCluster()));
		}

		return res;
	}
}
