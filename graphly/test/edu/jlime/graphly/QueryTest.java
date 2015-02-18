package edu.jlime.graphly;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.LocationMapper;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.TraversalResult;

public class QueryTest {
	public static void main(String[] args) throws Exception {
		Graphly g = Graphly.build(4);

		// long[][] adj = new long[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 0, 4 },
		// { 0, 5 }, { 3, 6 }, { 3, 7 }, { 1, 6 }, { 6, 1 }, { 7, 1 },
		// { 8, 1 }, { 9, 1 }, { 6, 11 }, { 6, 12 }, { 6, 13 }, { 7, 13 } };

		// long[][] test = new long[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 1, 4
		// },
		// { 2, 5 }, { 3, 6 }, { 7, 0 }, { 8, 0 }, { 9, 0 } };
		// TLongObjectHashMap<TLongArrayList> adj = new TLongObjectHashMap<>();
		// for (long[] ls : test) {
		// TLongArrayList l = adj.get(ls[0]);
		// if (l == null) {
		// l = new TLongArrayList();
		// adj.put(ls[0], l);
		// }
		// l.add(ls[1]);
		// }

		// Exploratory Count
		// TraversalResult res = g.v(new long[] { 0 })
		// .set("mapper", new LocationMapper()).as(Recommendation.class)
		// .exploratoryCount(100, 10, Dir.OUT, Dir.IN, Dir.OUT)
		// .submit(g.getJobClient().getCluster().getAnyExecutor());
		//
		// System.out.println("Exploratory Count: " + res);

		long[] target = new long[] { 0, 1, 2, 3 };

		// Random Walk
		// TraversalResult rwRes = g.v(target).set("mapper", new
		// LocationMapper())
		// .as(Recommendation.class).randomwalk("rw", 1000, 5)
		// .submit(g.getJobClient().getCluster().getAnyExecutor());
		// System.out.println("Random Walk: "
		// + g.collect("rw", -1, rwRes.vertices().toArray()));

		// HITS
		// TraversalResult tr = g.v(target).set("mapper", new LocationMapper())
		// .as(Recommendation.class).hits("auth", "hub", 5000, 10).exec();
		//
		// System.out.println("HITS");
		// System.out.println("Auth: "
		// + g.collect("auth", 10, tr.vertices().toArray()));
		// System.out.println("Hub: "
		// + g.collect("hub", 10, tr.vertices().toArray()));
		//
		// long[] salsatarget = new long[] { 0 };
		// //
		// // // Salsa
		// TraversalResult salsa = g.v(salsatarget).traverse(30, Dir.OUT,
		// Dir.OUT)
		// .set("mapper", new LocationMapper()).as(Recommendation.class)
		// .salsa("auth", "hub", 20, 10)
		// .submit(g.getJobClient().getCluster().getAnyExecutor());
		// //
		// System.out.println("SALSA");
		// System.out.println("Auth: "
		// + g.collect("auth", 10, salsa.vertices().toArray()));
		// System.out.println("Hub: "
		// + g.collect("hub", 10, salsa.vertices().toArray()));
		//
		// // // Salsa
		// TraversalResult salsarw = g.v(salsatarget)
		// .traverse(30, Dir.OUT, Dir.OUT)
		// .set("mapper", new LocationMapper()).as(Recommendation.class)
		// .salsaRW("authrw", "hubrw", 1000, 10)
		// .submit(g.getJobClient().getCluster().getAnyExecutor());
		// //
		// System.out.println("SALSA");
		// System.out.println("Auth: "
		// + g.collect("authrw", 10, salsarw.vertices().toArray()));
		// System.out.println("Hub: "
		// + g.collect("hubrw", 10, salsarw.vertices().toArray()));

		// // Salsa
		TraversalResult wtf = g.v(new long[] { 0 })
				.set("mapper", new LocationMapper()).as(Recommendation.class)
				.whotofollow("authwtf", "hubwtf", 5000, 10, 50)
				.submit(g.getJobClient().getCluster().getAnyExecutor());
		//
		System.out.println("WTF");
		System.out.println("Auth: "
				+ g.collect("authwtf", 10, wtf.vertices().toArray()));

		g.close();
	}
}
