package edu.jlime.graphly.util;

import java.util.Set;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.traversal.ValueResult;
import edu.jlime.util.Pair;

public class Algorithms {
	public static GraphlyRun ecHybrid() {
		return new GraphlyRun("ecHybrid") {
			public GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class)
						.exploratoryCountHybrid(Integer.MAX_VALUE, 10, "count", Dir.OUT, Dir.IN, Dir.OUT).asTraversal();
			}

			@Override
			public String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Exploratory Count Hybrid: \n" + res + "\n");
				//
				// builder.append("Exploratory Count FJ:");
				// Set<Pair<Long, Float>> set = g.topFloat("count", 10);
				// for (Pair<Long, Float> pair : set) {
				// builder.append(pair.getKey() + "=" + pair.getValue() + "\n");
				// }
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun friendLinkFJ() {
		return new GraphlyRun("friendLinkFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).friendLinkFJ(10, 3).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("FriendLink Fork Join: ");
				builder.append(res.toString() + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun friendLinkPregel() {
		return new GraphlyRun("friendLinkPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).friendLinkPregel("fl", 3)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("FriendLink Pregel: \n");
				{
					Set<Pair<Long, Float>> set = g.topFloat("fl", 10);
					for (Pair<Long, Float> pair : set) {
						builder.append(pair.getKey() + "=" + pair.getValue() + "\n");
					}
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun localPathFJ() {
		return new GraphlyRun("localPathFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).localPathFJ(0.1f, 10, Dir.BOTH)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Local Path Fork Join: \n");
				builder.append(res.toString() + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun localPathPregel() {
		return new GraphlyRun("localPathPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class)
						.localPathPregel("lp", Dir.BOTH, 0.1f).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Local Path Pregel: \n");
				{
					Set<Pair<Long, Float>> set = g.topFloat("lp", 10);
					for (Pair<Long, Float> pair : set) {
						builder.append(pair.getKey() + "=" + pair.getValue() + "\n");
					}
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun adamicFJ() {
		return new GraphlyRun("adamicFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).adamicFJ().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("AdamicAdar Fork Join:" + res.toString() + " \n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun adamicPregel() {
		return new GraphlyRun("adamicPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).adamicPregel().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("AdamicAdar Pregel:" + ((ValueResult) res).getRes() + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun jaccardFJ() {
		return new GraphlyRun("jaccardFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).jaccardFJ().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Jaccard Fork Join: " + res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun jaccardPregel() {
		return new GraphlyRun("jaccardPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).jaccardPregel().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Jaccard Pregel:" + ((ValueResult) res).getRes() + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun katzFJ() {
		return new GraphlyRun("katzFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).katzFJ(0.0001f, 4, 10)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Katz Fork Join: \n" + res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun katzPregel() {
		return new GraphlyRun("katzPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).katz("katz", 4, 0.0001f)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Katz Pregel: \n");
				{
					Set<Pair<Long, Float>> set = g.topFloat("katz", 10);
					for (Pair<Long, Float> pair : set) {
						builder.append(pair.getKey() + "=" + pair.getValue() + "\n");
					}
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun commonFJ() {
		return new GraphlyRun("commonFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).commonFJ().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Common Neighbours Fork Join: " + res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun commonPregel() {
		return new GraphlyRun("commonPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).commonNeighboursPregel()
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Common Neighbours Pregel: " + res + "\n");
				return builder.toString();
			}
		};
	}

	// protected static GraphlyRun wtfFJ() throws Exception {
	//
	// }
	//
	// protected static GraphlyRun wtfPregel() throws Exception {
	// }

	protected static GraphlyRun hitsFJ() {
		return new GraphlyRun("hitsFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).expand(Dir.BOTH, 100000).expand(Dir.BOTH, 1000)
						.as(Recommendation.class).hits("hits-auth", "hits-hub", 10).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Hits Fork Join: ");
				builder.append("Auth values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("hits-auth", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("Hub values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("hits-hub", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("\nSum");
				{
					float sum = graph.sumFloat("hits-auth");
					builder.append("auth: " + sum + "\n");

				}

				{
					float sum = graph.sumFloat("hits-hub");
					builder.append("hub: " + sum + "\n");
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun hitsPregel() {
		return new GraphlyRun("hitsPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).expand(Dir.BOTH, 100000).expand(Dir.BOTH, 1000)
						.as(Recommendation.class).hitsPregel("hits-auth", "hits-hub", 10).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Hits Pregel: ");
				builder.append("Auth values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("hits-auth", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("Hub values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("hits-hub", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("\nSum");
				{
					float sum = graph.sumFloat("hits-auth");
					builder.append("auth: " + sum + "\n");

				}

				{
					float sum = graph.sumFloat("hits-hub");
					builder.append("hub: " + sum + "\n");
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun salsaFJ() {
		return new GraphlyRun("salsaFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).expand(Dir.BOTH, 1000).expand(Dir.BOTH, 1000)
						.as(Recommendation.class).salsa("salsa-auth", "salsa-hub", 10).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Salsa Pregel: ");
				builder.append("Auth values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-auth", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("Hub values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-hub", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("\nSum");
				{
					float sum = graph.sumFloat("salsa-auth");
					builder.append("auth: " + sum + "\n");

				}

				{
					float sum = graph.sumFloat("salsa-hub");
					builder.append("hub: " + sum + "\n");
				}
				return builder.toString();
			}
		};
	}

	public static GraphlyRun salsaPregel() {
		return new GraphlyRun("salsaPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).expand(Dir.BOTH, 1000).expand(Dir.BOTH, 1000)
						.as(Recommendation.class).salsaPregel("salsa-auth", "salsa-hub", 10).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Salsa Pregel: ");
				builder.append("Auth values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-auth", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("Hub values");
				{
					Set<Pair<Long, Float>> topFloat = graph.topFloat("salsa-hub", 10);
					for (Pair<Long, Float> pair : topFloat) {
						builder.append(pair.left + "=" + pair.right + "\n");
					}
				}

				builder.append("\nSum");
				{
					float sum = graph.sumFloat("salsa-auth");
					builder.append("auth: " + sum + "\n");

				}

				{
					float sum = graph.sumFloat("salsa-hub");
					builder.append("hub: " + sum + "\n");
				}
				return builder.toString();
			}
		};
	}

	public static GraphlyRun ecFJ() {
		return new GraphlyRun("ecFJ") {
			public GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class)
						.exploratoryCount(Integer.MAX_VALUE, 10, Dir.OUT, Dir.IN, Dir.OUT).asTraversal();
			}

			@Override
			public String printResult(TraversalResult res, GraphlyGraph g) throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Exploratory Count: \n" + res + "\n");
				//
				// builder.append("Exploratory Count FJ:");
				// Set<Pair<Long, Float>> set = g.topFloat("count", 10);
				// for (Pair<Long, Float> pair : set) {
				// builder.append(pair.getKey() + "=" + pair.getValue() + "\n");
				// }
				return builder.toString();
			}
		};
	}

	public static GraphlyRun ecPregel() {
		return new GraphlyRun("ecPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph, Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper).as(Recommendation.class).exploratoryCountPregel(10)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g) throws Exception {

				StringBuilder builder = new StringBuilder();
				builder.append("Exploratory Count Pregel: \n" + res + "\n");
				//
				// builder.append("Exploratory Count FJ:");
				// Set<Pair<Long, Float>> set = g.topFloat("count", 10);
				// for (Pair<Long, Float> pair : set) {
				// builder.append(pair.getKey() + "=" + pair.getValue() + "\n");
				// }
				return builder.toString();
				// StringBuilder builder = new StringBuilder();
				// builder.append("Exploratory Count Pregel: \n");
				// {
				// Set<Pair<Long, Float>> set = g.topFloat("count", 10);
				// for (Pair<Long, Float> pair : set) {
				// builder.append(pair.getKey() + "=" + pair.getValue()
				// + "\n");
				// }
				// }
				// return builder.toString();
			}
		};
	}
}
