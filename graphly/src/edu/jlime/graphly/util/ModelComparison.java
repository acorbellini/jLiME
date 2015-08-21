package edu.jlime.graphly.util;

import java.util.Set;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.MapperFactory;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.ValueResult;
import edu.jlime.util.Pair;

public class ModelComparison {

	private static final String START_NODE = "GridCluster10";

	private static boolean print_results = false;

	public static void main(String[] args) throws Exception {

		// GraphlyServerFactory fact = GraphlyServerFactory.loopback(args[0]);
		// String graphName = args[1];

		GraphlyServerFactory fact = GraphlyServerFactory.distributed(args[0],
				args[1], args[2]);
		String graphName = args[3];

		GraphlyExperiment[] exp = new GraphlyExperiment[] {
				new GraphlyExperiment("EC_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.ecFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("EC_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.ecPregel(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("FriendLink_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.friendLinkFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("FriendLink_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.friendLinkPregel(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("LocalPath_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.localPathFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("LocalPath_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.localPathPregel(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("Katz_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.katzFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("Katz_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.katzPregel(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("SALSA_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.salsaFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("SALSA_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.salsaPregel(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("HITS_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.hitsFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("HITS_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.hitsPregel(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("AdamicAdar_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.adamicFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("AdamicAdar_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.adamicPregel(graph);
					}
				}, fact, graphName),

				new GraphlyExperiment("Jaccard_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.jaccardFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("Jaccard_Pregel", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.jaccardPregel(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("CommonNeighbours_FJ", new GraphlyRun() {
					public void run(GraphlyGraph graph) throws Exception {
						ModelComparison.commonFJ(graph);
					}
				}, fact, graphName),
				new GraphlyExperiment("CommonNeighbours_Pregel",
						new GraphlyRun() {
							public void run(GraphlyGraph graph)
									throws Exception {
								ModelComparison.commonPregel(graph);
							}
						}, fact, graphName) };

		for (GraphlyExperiment graphlyExperiment : exp) {
			graphlyExperiment.execute();
		}
	}

	protected static void friendLinkPregel(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.friendLinkPregel("fl", 3)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("FriendLink Pregel: ");
			{
				Set<Pair<Long, Float>> set = graph.topFloat("fl", 10);
				for (Pair<Long, Float> pair : set) {
					System.out.println(pair.getKey() + "=" + pair.getValue());
				}
			}
		}
	}

	protected static void friendLinkFJ(GraphlyGraph graph) throws Exception {
		CountResult res = (CountResult) graph
				.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.friendLinkFJ(10)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("FriendLink Fork Join: ");
			System.out.println(res.toString());
		}
	}

	protected static void localPathFJ(GraphlyGraph graph) throws Exception {
		CountResult res = (CountResult) graph
				.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.localPathFJ(0.1f, 10)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Local Path Fork Join: ");
			System.out.println(res.toString());
		}
	}

	protected static void localPathPregel(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.localPathPregel("lp", 0.1f)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		;
		if (print_results) {
			System.out.println("LocalPath Pregel: ");
			{
				Set<Pair<Long, Float>> set = graph.topFloat("lp", 10);
				for (Pair<Long, Float> pair : set) {
					System.out.println(pair.getKey() + "=" + pair.getValue());
				}
			}
		}

	}

	protected static void adamicFJ(GraphlyGraph graph) throws Exception {
		ValueResult res = (ValueResult) graph
				.v(1, 2, 3, 4, 5, 6, 7)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.adamicFJ()
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("AdamicAdar FJ: " + res.getRes());
		}
	}

	protected static void adamicPregel(GraphlyGraph graph) throws Exception {
		ValueResult val = (ValueResult) graph
				.v(1, 2, 3, 4, 5, 6, 7)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.adamicPregel()
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {

			System.out.println("Adamic Adar Pregel: " + val.getRes());
		}
	}

	protected static void jaccardFJ(GraphlyGraph graph) throws Exception {
		ValueResult res = (ValueResult) graph
				.v(1, 2, 3, 4, 5, 6, 7)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.jaccardFJ()
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {

			System.out.println("Jaccard FJ: " + res.getRes());
		}
	}

	protected static void jaccardPregel(GraphlyGraph graph) throws Exception {
		ValueResult val = (ValueResult) graph
				.v(1, 2, 3, 4, 5, 6, 7)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.jaccardPregel()
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Jaccard Pregel: " + val.getRes());
		}

	}

	protected static void katzFJ(GraphlyGraph graph) throws Exception {
		CountResult res = (CountResult) graph
				.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.katzFJ(0.1f, 4, 10)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Katz ForkJoin: \n" + res.toString());
		}
	}

	protected static void katzPregel(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.katz("katz", 4, 0.1f)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Katz Pregel:");
			{
				Set<Pair<Long, Float>> set = graph.topFloat("katz", 10);
				for (Pair<Long, Float> pair : set) {
					System.out.println(pair.getKey() + "=" + pair.getValue());
				}
			}
		}
	}

	protected static void commonFJ(GraphlyGraph graph) throws Exception {
		ValueResult res = (ValueResult) graph
				.v(1, 2, 3, 4, 5, 6, 7)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.commonFJ()
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Common Neighbours FJ: " + res.getRes());
		}
	}

	protected static void commonPregel(GraphlyGraph graph) throws Exception {
		ValueResult val = (ValueResult) graph
				.v(1, 2, 3, 4, 5, 6, 7)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.commonNeighboursPregel()
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Common Neighbours Pregel: " + val.getRes());
		}
	}

	protected static void wtfFJ(GraphlyGraph graph) throws Exception {

	}

	protected static void wtfPregel(GraphlyGraph graph) throws Exception {
	}

	protected static void hitsFJ(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.expand(Dir.BOTH, 100000)
				.expand(Dir.BOTH, 1000)
				.as(Recommendation.class)
				.hits("hits-auth", "hits-hub", 10)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Hits Fork Join: ");
			System.out.println("Auth values");
			{
				Set<Pair<Long, Float>> topFloat = graph.topFloat("hits-auth",
						10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("Hub values");
			{
				Set<Pair<Long, Float>> topFloat = graph
						.topFloat("hits-hub", 10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("\nSum");
			{
				float sum = graph.sumFloat("hits-auth");
				System.out.println("auth: " + sum);

			}

			{
				float sum = graph.sumFloat("hits-hub");
				System.out.println("hub: " + sum);
			}
		}
	}

	protected static void hitsPregel(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.expand(Dir.BOTH, 100000)
				.expand(Dir.BOTH, 1000)
				.as(Recommendation.class)
				.hitsPregel("hits-auth", "hits-hub", 10)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("HITS Pregel: ");
			System.out.println("Auth values");
			{
				Set<Pair<Long, Float>> topFloat = graph.topFloat("hits-auth",
						10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("Hub values");
			{
				Set<Pair<Long, Float>> topFloat = graph
						.topFloat("hits-hub", 10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("\nSum");
			{
				float sum = graph.sumFloat("hits-auth");
				System.out.println("auth: " + sum);
			}

			{
				float sum = graph.sumFloat("hits-hub");
				System.out.println("hub: " + sum);
			}
		}
	}

	protected static void salsaFJ(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.expand(Dir.BOTH, 100000)
				.expand(Dir.BOTH, 1000)
				.add(1)
				.as(Recommendation.class)
				.salsa("salsa-auth", "salsa-hub", 10)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Salsa Fork Join: ");
			System.out.println("Auth values");
			{
				Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-auth",
						10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("Hub values");
			{
				Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-hub",
						10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("\nSum");
			{
				float sum = graph.sumFloat("salsa-auth");
				System.out.println("auth: " + sum);
			}

			{
				float sum = graph.sumFloat("salsa-hub");
				System.out.println("hub: " + sum);
			}
		}
	}

	protected static void salsaPregel(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.expand(Dir.BOTH, 100000)
				.expand(Dir.BOTH, 1000)
				.as(Recommendation.class)
				.salsaPregel("salsa-auth", "salsa-hub", 10)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Salsa Pregel: ");
			System.out.println("Auth values");
			{
				Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-auth",
						10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("Hub values");
			{
				Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-hub",
						10);
				for (Pair<Long, Float> pair : topFloat) {
					System.out.println(pair.left + "=" + pair.right);
				}
			}

			System.out.println("\nSum");
			{
				float sum = graph.sumFloat("salsa-auth");
				System.out.println("auth: " + sum);
			}

			{
				float sum = graph.sumFloat("salsa-hub");
				System.out.println("hub: " + sum);
			}
		}
	}

	private static void ecFJ(GraphlyGraph graph) throws Exception {
		CountResult res = (CountResult) graph
				.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.exploratoryCount(Integer.MAX_VALUE, 10, "count", Dir.OUT,
						Dir.IN, Dir.OUT)
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));

		if (print_results) {
			System.out.println("Exploratory Count: \n" + res.toString());
		}
	}

	public static void ecPregel(GraphlyGraph graph) throws Exception {
		graph.v(1)
				.set("mapper", MapperFactory.location())
				.as(Recommendation.class)
				.exploratoryCountPregel()
				.submit(graph.getJobClient().getCluster().getByName(START_NODE));
		if (print_results) {
			System.out.println("Exploratory Count Pregel:");
			Set<Pair<Long, Float>> set = graph.topFloat("count", 10);
			for (Pair<Long, Float> pair : set) {
				System.out.println(pair.getKey() + "=" + pair.getValue());
			}
		}
	}
}
