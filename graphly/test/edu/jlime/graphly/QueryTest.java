package edu.jlime.graphly;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.LocationMapper;
import edu.jlime.graphly.recommendation.Recommendation;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.list.array.TLongArrayList;

public class QueryTest {
	public static void main(String[] args) throws Exception {
		Graphly g = Graphly.build(4);

		// long[][] adj = new long[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 0, 4 },
		// { 0, 5 }, { 3, 6 }, { 3, 7 }, { 1, 6 }, { 6, 1 }, { 7, 1 },
		// { 8, 1 }, { 9, 1 }, { 6, 11 }, { 6, 12 }, { 6, 13 }, { 7, 13 } };

		long[][] adj = new long[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 1, 4 },
				{ 2, 5 }, { 3, 6 }, { 7, 0 }, { 8, 0 }, { 9, 0 } };

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

		// Exploratory Count
		TraversalResult res = g.v(new long[] { 0 })
				.set("mapper", new LocationMapper()).as(Recommendation.class)
				.exploratoryCount(Dir.OUT, Dir.IN, Dir.OUT)
				.submit(g.getJobClient().getCluster().getAnyExecutor());

		System.out.println(res);

		long[] target = new long[] { 0, 1, 2, 3 };
		// Random Walk
		TraversalResult rwRes = g.v(target).set("mapper", new LocationMapper())
				.as(Recommendation.class).randomwalk("rw", 1000, 5)
				.submit(g.getJobClient().getCluster().getAnyExecutor());
		System.out.println(g.collect("rw", rwRes.vertices().toArray()));

		// HITS
		TraversalResult tr = g.v(target).set("mapper", new LocationMapper())
				.as(Recommendation.class).hits("auth", "hub", 1000, 10).exec();
		System.out.println(g.collect("auth", tr.vertices().toArray()));
		System.out.println(g.collect("hub", tr.vertices().toArray()));

		long[] salsatarget = new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

		// Salsa
		TraversalResult salsa = g.v(salsatarget)
				.set("mapper", new LocationMapper()).as(Recommendation.class)
				.salsa("auth", "hub", 50000, 10)
				.submit(g.getJobClient().getCluster().getAnyExecutor());

		System.out.println("Auth"
				+ g.collect("auth", salsa.vertices().toArray()));

		System.out
				.println("Hub" + g.collect("hub", salsa.vertices().toArray()));

		g.close();
	}
}
