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
	private static final int KATZ_DEPTH = 4;
	private static final int FRIENDLINK_DEPTH = 4;
	private static final int HITS_NEIGHBOURS = 100000;
	private static final int SALSA_NEIGHBOURS = 100000;

	public static GraphlyRun ecHybrid() {
		return new GraphlyRun("ecHybrid") {
			public GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.exploratoryCountHybrid(Integer.MAX_VALUE, 10, "count",
								Dir.OUT, Dir.IN, Dir.OUT)
						.asTraversal();
			}

			@Override
			public String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Exploratory Count Hybrid: \n" + res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun friendLinkFJ() {
		return new GraphlyRun("friendLinkFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.friendLinkFJ(10, FRIENDLINK_DEPTH, Dir.OUT)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("FriendLink Fork Join: \n");
				builder.append(res.toString() + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun friendLinkPregel() {
		return new GraphlyRun("friendLinkPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.friendLinkPregel("fl", FRIENDLINK_DEPTH, Dir.OUT)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("FriendLink Pregel: \n");
				{
					Set<Pair<Long, Float>> set = g.topFloat("fl", 10);
					for (Pair<Long, Float> pair : set) {
						builder.append(
								pair.getKey() + "=" + pair.getValue() + "\n");
					}
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun localPathFJ() {
		return new GraphlyRun("localPathFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).localPathFJ(0.1f, 10, Dir.OUT)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
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
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.localPathPregel("lp", Dir.OUT, 0.1f).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Local Path Pregel: \n");
				{
					Set<Pair<Long, Float>> set = g.topFloat("lp", 10);
					for (Pair<Long, Float> pair : set) {
						builder.append(
								pair.getKey() + "=" + pair.getValue() + "\n");
					}
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun adamicFJ() {
		return new GraphlyRun("adamicFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).adamicFJ().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append(
						"AdamicAdar Fork Join:" + res.toString() + " \n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun adamicPregel() {
		return new GraphlyRun("adamicPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).adamicPregel().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("AdamicAdar Pregel:"
						+ ((ValueResult) res).getRes() + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun jaccardFJ() {
		return new GraphlyRun("jaccardFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).jaccardFJ().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Jaccard Fork Join: " + res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun jaccardPregel() {
		return new GraphlyRun("jaccardPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).jaccardPregel().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Jaccard Pregel:" + ((ValueResult) res).getRes()
						+ "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun katzFJ() {
		return new GraphlyRun("katzFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.katzFJ(0.0001f, KATZ_DEPTH, 10, Dir.OUT).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Katz Fork Join: \n" + res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun katzPregel() {
		return new GraphlyRun("katzPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.katz("katz", KATZ_DEPTH, 0.0001f, Dir.OUT)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Katz Pregel: \n");
				{
					Set<Pair<Long, Float>> set = g.topFloat("katz", 10);
					for (Pair<Long, Float> pair : set) {
						builder.append(
								pair.getKey() + "=" + pair.getValue() + "\n");
					}
				}
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun commonFJ() {
		return new GraphlyRun("commonFJ") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).commonFJ().asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Common Neighbours Fork Join: " + res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun commonPregel() {
		return new GraphlyRun("commonPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).commonNeighboursPregel()
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
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
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.expand(Dir.BOTH, HITS_NEIGHBOURS)
						.expand(Dir.BOTH, HITS_NEIGHBOURS)
						.as(Recommendation.class).hits(10, 10).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("HITS Fork Join: \n");
				builder.append(res + "\n");
				return builder.toString();
			}
		};

	}

	protected static GraphlyRun hitsPregel() {
		return new GraphlyRun("hitsPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.expand(Dir.BOTH, HITS_NEIGHBOURS)
						.expand(Dir.BOTH, HITS_NEIGHBOURS)
						.as(Recommendation.class)
						.hitsPregel("hits-auth", "hits-hub", 10, 10)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Hits Pregel: \n");
				builder.append(res + "\n");
				return builder.toString();
			}
		};
	}

	protected static GraphlyRun salsaFJ() {
		return new GraphlyRun("salsaFJ") {
			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.expand(Dir.BOTH, SALSA_NEIGHBOURS)
						.expand(Dir.BOTH, SALSA_NEIGHBOURS)
						.as(Recommendation.class).salsa(10, 10).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Salsa Fork Join: \n");
				builder.append(res + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun salsaPregel() {
		return new GraphlyRun("salsaPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.expand(Dir.BOTH, SALSA_NEIGHBOURS)
						.expand(Dir.BOTH, SALSA_NEIGHBOURS)
						.as(Recommendation.class)
						.salsaPregel("salsa-auth", "salsa-hub", 10, 10)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Salsa Pregel: \n");
				builder.append(res + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun ecFJ() {
		return new GraphlyRun("ecFJ") {
			public GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.exploratoryCount(Integer.MAX_VALUE, 10, Dir.OUT,
								Dir.IN, Dir.OUT)
						.asTraversal();
			}

			@Override
			public String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Exploratory Count: \n" + res + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun ecPregel() {
		return new GraphlyRun("ecPregel") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class).exploratoryCountPregel(10)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {

				StringBuilder builder = new StringBuilder();
				builder.append("Exploratory Count Pregel: \n" + res + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun friendLinkHybrid() {
		return new GraphlyRun("friendLinkHybrid") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.friendLinkHybrid(10, FRIENDLINK_DEPTH, Dir.OUT)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("FriendLink Hybrid: \n");
				builder.append(res.toString() + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun localPathHybrid() {
		return new GraphlyRun("localPathHybrid") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.localPathHybrid(0.1f, 10, Dir.OUT).asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Local Path Hybrid: \n");
				builder.append(res.toString() + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun katzHybrid() {
		return new GraphlyRun("katzHybrid") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.as(Recommendation.class)
						.katzHybrid(0.0001f, KATZ_DEPTH, 10, Dir.OUT)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph g)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Katz Hybrid: \n");
				builder.append(res.toString() + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun salsaHybrid() {
		return new GraphlyRun("salsaHybrid") {
			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.expand(Dir.BOTH, SALSA_NEIGHBOURS)
						.expand(Dir.BOTH, SALSA_NEIGHBOURS)
						.as(Recommendation.class)
						.salsaHybrid("salsa-auth", "salsa-hub", 10, 10)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Salsa Hybrid: \n");
				builder.append(res + "\n");
				return builder.toString();
			}
		};
	}

	public static GraphlyRun hitsHybrid() {
		return new GraphlyRun("hitsHybrid") {

			@Override
			GraphlyTraversal run(long[] users, GraphlyGraph graph,
					Mapper mapper) throws Exception {
				return graph.v(users).set("mapper", mapper)
						.expand(Dir.BOTH, HITS_NEIGHBOURS)
						.expand(Dir.BOTH, HITS_NEIGHBOURS)
						.as(Recommendation.class)
						.hitsHybrid("hits-auth", "hits-hub", 10, 10)
						.asTraversal();
			}

			@Override
			String printResult(TraversalResult res, GraphlyGraph graph)
					throws Exception {
				StringBuilder builder = new StringBuilder();
				builder.append("Hits Hybrid: \n");
				builder.append(res + "\n");
				return builder.toString();
			}
		};
	}
}
