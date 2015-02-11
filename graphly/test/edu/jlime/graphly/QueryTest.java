package edu.jlime.graphly;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.LocationMapper;
import edu.jlime.graphly.traversal.recommendation.Recommendation;
import gnu.trove.list.array.TLongArrayList;

public class QueryTest {
	public static void main(String[] args) throws Exception {
		Graphly g = Graphly.build(4);

		long[][] adj = new long[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 0, 4 },
				{ 0, 5 }, { 6, 1 }, { 7, 1 }, { 8, 1 }, { 9, 1 }, { 6, 11 },
				{ 6, 12 }, { 6, 13 }, { 7, 13 } };

		Map<Long, TLongArrayList> out = new HashMap<>();
		Map<Long, TLongArrayList> in = new HashMap<>();

		for (int i = 0; i < adj.length; i++) {
			TLongArrayList outList = out.get(adj[i][0]);
			if (outList == null) {
				outList = new TLongArrayList();
				out.put(adj[i][0], outList);
			}
			outList.add(adj[i][1]);
			TLongArrayList inList = in.get(adj[i][1]);
			if (inList == null) {
				inList = new TLongArrayList();
				in.put(adj[i][1], inList);
			}
			inList.add(adj[i][0]);
		}

		for (Entry<Long, TLongArrayList> e : out.entrySet()) {
			g.addEdges(e.getKey(), Dir.OUT, e.getValue().toArray());
		}

		for (Entry<Long, TLongArrayList> e : in.entrySet()) {
			g.addEdges(e.getKey(), Dir.IN, e.getValue().toArray());
		}

		for (Entry<Long, TLongArrayList> e : out.entrySet()) {
			Long vid = e.getKey();
			long[] dbList = g.getEdges(Dir.OUT, vid);
			assert (Arrays.equals(dbList, e.getValue().toArray()));
		}

		for (Entry<Long, TLongArrayList> e : in.entrySet()) {
			Long vid = e.getKey();
			long[] dbList = g.getEdges(Dir.IN, vid);
			assert (Arrays.equals(dbList, e.getValue().toArray()));
		}

		long[] target = new long[] { 0 };

		Object res = g.v(target).set("mapper", new LocationMapper())
				.as(Recommendation.class)
				.exploratoryCount(Dir.OUT, Dir.IN, Dir.OUT)
				.submit(g.getJobClient().getCluster().getAnyExecutor());
		
		Object randomWalk = g.v(target).set("mapper", new LocationMapper())
				.as(Recommendation.class).randomwalk(50000, 5)
				.submit(g.getJobClient().getCluster().getAnyExecutor());

		System.out.println(res);
		g.close();
	}
}
