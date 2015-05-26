package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.MapperFactory;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Pregel;

public class QueryTest {

	public static void main(String[] args) throws Exception {
		Graphly graphly = Graphly.build(4);

		// System.out.println(graphly.listGraphs());
		//
		final GraphlyGraph g = graphly.getGraph(args[0]);
		//
		// long init = System.currentTimeMillis();
		// Pregel tr = g.v().set("mapper", MapperFactory.location())
		// .as(Recommendation.class).hitsPregel("auth", "hub", 50)
		// .as(Pregel.class);
		//
		// tr.getConfig().threads(2).queue(1000);
		//
		// tr.exec();
		//
		// System.out.println("Time: " + (System.currentTimeMillis() - init));
		// {
		// float sum = 0;
		// List<Float> vals = g.gather(new SumFloatPropertiesGather("auth"));
		// for (Float d : vals) {
		// sum += d;
		// }
		//
		// System.out.println(sum);
		// }
		//
		// {
		// float sum = 0;
		// List<Float> vals = g.gather(new SumFloatPropertiesGather("hub"));
		// for (Float d : vals) {
		// sum += d;
		// }
		// System.out.println(sum);
		// }
		// {
		// System.out.println("\nTop Auth\n----------");
		// Set<Pair<Long, Float>> top = g.topFloat("auth", 10);
		// for (Pair<Long, Float> pair : top) {
		// System.out.println(pair.left + ":" + pair.right);
		// }
		// }
		// {
		// System.out.println("\nTop Hub\n----------");
		// Set<Pair<Long, Float>> top = g.topFloat("hub", 10);
		// for (Pair<Long, Float> pair : top) {
		// System.out.println(pair.left + ":" + pair.right);
		// }
		// }
		//
		// System.out.println("\nSum");
		// {
		// float sum = g.sumFloat("auth");
		// float quad = g.quadSumFloat("auth");
		// System.out.println("auth: " + sum);
		// System.out.println("auth quad: " + quad);
		// System.out.println("norm l2: " + sum / Math.sqrt(quad));
		// }
		//
		// {
		// float sum = g.sumFloat("hub");
		// float quad = g.quadSumFloat("hub");
		// System.out.println("hub: " + sum);
		// System.out.println("hub quad: " + quad);
		// System.out.println("norm l2: " + sum / Math.sqrt(quad));
		//
		// }

		// int count = 0;
		// VertexList vlist = g.vertices();
		// for (Long long1 : vlist) {
		// count++;
		// }
		// System.out.println(count);

		// System.out.println("Counting Vertices");
		// int vertexCount = g.getVertexCount();

		// System.out.println("Finished Counting Vertices: " + vertexCount);

		// g.setDefaultFloat("pagerank", 1f / vertexCount);
		// g.setDefaultFloat("ranksource", .85f);
		//
		// {
		// float sum = 0;
		// List<Float> vals = g
		// .gather(new SumFloatPropertiesGather("pagerank"));
		// for (Float d : vals) {
		// sum += d;
		// }
		//
		// System.out.println(sum);
		// }
		long init = System.currentTimeMillis();
		Pregel pagerank = g.v().set("mapper", MapperFactory.location())
				.as(Recommendation.class).pagerank("pagerank", 30, 1E-5f)
				.as(Pregel.class);

		pagerank.getConfig().threads(2).queue(1000);

		pagerank.exec();

		System.out.println(System.currentTimeMillis() - init);

		float sum = g.sumFloat("pagerank");

		System.out.println(sum);

		// final AtomicDouble sum = new AtomicDouble(0d);
		// // If everything's alright, the sum should be 1 (Or near)
		// ForkJoinPool pool = new ForkJoinPool();
		// for (final Long v : g.vertices()) {
		// pool.execute(new Runnable() {
		// @Override
		// public void run() {
		// try {
		// sum.addAndGet(g.getFloat(v, "pagerank"));
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// });
		// }
		//
		// pool.shutdown();
		// pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

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

		// System.out.println(g.getVertexCount());
		// System.out.println(g.getVertexCount());
		//
		// int cont = 0;
		// List<Long> vs = new ArrayList<>();
		// for (Long v : g.vertices()) {
		// System.out.println(v);
		// vs.add(v);
		// cont++;
		// }
		// System.out.println(cont);
		// Collections.sort(vs);
		// for (int i = 0; i < args.length; i++) {
		// if (vs.get(i) != i)
		// System.out.println("Not equal");
		// }
		// System.out.println(vs);

		// // Exploratory Count
		// Mapper rr = MapperFactory.rr();
		// TraversalResult res = g.v(new long[] { 0 }).set("mapper", rr)
		// .as(Recommendation.class)
		// .exploratoryCount(100, 10, Dir.OUT, Dir.IN, Dir.OUT).exec();
		// //
		// System.out.println("Exploratory Count: " + res);
		// //
		// // long[] target = new long[] { 3657372, 6388, 3653225, 4530, 1020022
		// };
		// //
		// // Random Walk
		// // TraversalResult rwRes = g.v(target).set("mapper", new
		// // LocationMapper())
		// // .as(Recommendation.class).randomwalk("rw", 1000, 0.8f)
		// // .submit(g.getJobClient().getCluster().getAnyExecutor());
		// // System.out.println("Random Walk: "
		// // + g.collect("rw", -1, rwRes.vertices().toArray()));
		// //
		// // HITS
		// // TraversalResult tr = g.v(target).set("mapper", new
		// LocationMapper())
		// // .as(Recommendation.class).hits("auth", "hub", 200000, 10)
		// // .exec();
		// //
		// // System.out.println("HITS");
		// // System.out.println("Auth: "
		// // + g.collect("auth", 10, tr.vertices().toArray()));
		// // System.out.println("Hub: "
		// // + g.collect("hub", 10, tr.vertices().toArray()));
		// // //
		// long[] salsatarget = new long[] { 0, 3657372 };
		// // //
		// // Salsa
		// TraversalResult salsa = g.v(salsatarget).traverse(500, Dir.OUT)
		// .set("mapper", rr).as(Recommendation.class)
		// .salsa("auth", "hub", 20000).exec();
		// //
		// System.out.println("SALSA");
		// System.out.println("Auth: "
		// + g.collect("auth", 10, salsa.vertices().toArray()));
		// System.out.println("Hub: "
		// + g.collect("hub", 10, salsa.vertices().toArray()));
		// //
		// // // Salsa
		// // TraversalResult salsarw = g.v(salsatarget)
		// // .traverse(30, Dir.OUT, Dir.OUT)
		// // .set("mapper", new LocationMapper()).as(Recommendation.class)
		// // .salsaRW("authrw", "hubrw", 1000, 10)
		// // .submit(g.getJobClient().getCluster().getAnyExecutor());
		// // //
		// // System.out.println("SALSA RandomWalk");
		// // System.out.println("Auth: "
		// // + g.collect("authrw", 10, salsarw.vertices().toArray()));
		// // System.out.println("Hub: "
		// // + g.collect("hubrw", 10, salsarw.vertices().toArray()));
		//
		// // WhoToFollow
		// TraversalResult wtf = g
		// .v(new long[] { 0, 3657372 })
		// .set("mapper", rr)
		// .as(Recommendation.class)
		// .whotofollow("authwtf", "hubwtf", 10000, 0.3f, 500, 50, 0.7f,
		// 100).exec();
		//
		// System.out.println("WTF");
		// System.out.println("wtf-auth:\n" + wtf);
		//
		// g.v().as(Pregel.class).execute(new
		// PageRank(g.getVertexCount())).exec();
		//
		graphly.close();
	}
}
