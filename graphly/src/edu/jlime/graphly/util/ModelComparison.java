package edu.jlime.graphly.util;

import java.util.Arrays;

import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.jobs.MapperFactory;
import edu.jlime.graphly.util.GraphlyExperiment.ExperimentResult;

public class ModelComparison {

	private static final String START_NODE = "GridCluster10";

	private static final int REPS = 1;

	private static final boolean PRINT = true;

	// @formatter:off
	private static final Mapper[] MAPPERS = new Mapper[] { 
	   MapperFactory.location(),
//	   MapperFactory.rr(),
//	   MapperFactory.criteria("jvminfo.mem.max", false),
//	   MapperFactory.criteria("jvminfo.mem.freemax", true),
	// MapperFactory.hybrid(new float[] { 0.5f, 0.5f },
	// MapperFactory.location(),
	// MapperFactory.simple("jvminfo.mem.freemax", true)),
	// MapperFactory.hybrid(new float[] { 0.7f, 0.3f },
	// MapperFactory.location(),
	// MapperFactory.simple("jvminfo.mem.freemax", true)),
	// MapperFactory.hybrid(new float[] { 0.9f, 0.1f },
	// MapperFactory.location(),
	// MapperFactory.simple("jvminfo.mem.freemax", true)),
	// MapperFactory.hybrid(new float[] { 1f, 0f },
	// MapperFactory.location(),
	// MapperFactory.simple("jvminfo.mem.freemax", true)),

	};
	// @formatter:on

	// @formatter:off
	private static final long[] USERS = new long[] {
//			1,
			// by-followers
//			1811269,
//			6035057,
//			1653,
//			1025811,
//			1437,
			1829999, 
//			5874844,
//			1803885,
//			5925043, 
//			1037948,
//			// by Followees
//			2589879,
//			1771195,
//			1008721, 
//			269339,
//			3806,
//			3663441,
//			2581430,
//			5299,
//			21407,
//			1786295,
//			IS>0.4 <0.6 sorted by followees
//			3653185,
//			3117695,
//			3154485,
//			1020022,
//			3655067,
//			31907,
//			3137321,
//			7994,
//			3629842,
//			1004282
	};
	// @formatter:on

	private static final long[][] GROUPS = new long[][] {
			// new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
			// { 1811269, 6035057, 1653, 1025811, 1437, 1829999, 5874844,
			// 1803885,
			// 5925043, 1037948 }, // by
			// // followers
			// { 1008721, 269339, 3806, 3663441, 2581430, 1803885, 5299, 21407,
			// 1786295, 1653 }, // by
			// // followees
			// { 3653185, 3117695, 3154485, 1020022, 3655067, 31907, 3137321,
			// 7994,
			// 3629842, 1004282 } // by
	};

	private boolean print_results;

	private GraphlyServerFactory fact;

	private String graphName;

	private String startNode;

	private int reps;

	// public ModelComparison(GraphlyServerFactory fact, String graphName,
	// boolean print_results) {
	// this(fact, graphName, print_results, null, 1);
	// }

	public ModelComparison(GraphlyServerFactory fact, String graphName,
			boolean print_results, String startNode, int reps) {
		this.startNode = startNode;
		this.graphName = graphName;
		this.fact = fact;
		this.print_results = print_results;
		this.reps = reps;
	}

	public static void main(String[] args) throws Exception {

		if (args[0].equals("local")) {
			GraphlyServerFactory fact = GraphlyServerFactory.loopback(args[1]);
			String graphName = args[2];
			new ModelComparison(fact, graphName, true, null, 0).run();
		} else {
			GraphlyServerFactory fact = GraphlyServerFactory.distributed(
					args[1], args[2], args[3], new Integer(args[4]));
			String graphName = args[5];
			new ModelComparison(fact, graphName, PRINT, START_NODE, REPS).run();
		}
	}

	private void run() throws Exception {
		// @formatter:off
		GraphlyRun[] exp = new GraphlyRun[] {
//				Algorithms.ecFJ(),				
//				Algorithms.ecPregel(),
//				Algorithms.ecHybrid(),
//				Algorithms.friendLinkFJ(),
//				Algorithms.friendLinkPregel(),
//				Algorithms.friendLinkHybrid(),
//				Algorithms.localPathFJ(),
//				Algorithms.localPathPregel(), 
//				Algorithms.localPathHybrid(),
//				Algorithms.katzFJ(),
//				Algorithms.katzPregel(),
//				Algorithms.katzHybrid(),
//				Algorithms.salsaFJ(),
				Algorithms.salsaPregel(),
				Algorithms.salsaHybrid(),
//				Algorithms.hitsFJ(),				
//				Algorithms.hitsPregel(),
//				Algorithms.hitsHybrid()
		};
		
		GraphlyRun[] groups_exp = new GraphlyRun[] { 
//				Algorithms.adamicFJ(),
//				Algorithms.adamicPregel(), 
//				Algorithms.jaccardFJ(),
//				Algorithms.jaccardPregel(), 
//				Algorithms.commonFJ(),
//				Algorithms.commonPregel() 
		};
		// @formatter:on

		for (GraphlyRun run : exp) {
			for (Mapper mapper : MAPPERS) {
				for (long user : USERS) {
					ExperimentResult expRes = GraphlyExperiment.exec(reps,
							new long[] { user }, graphName, fact, run,
							print_results, startNode, mapper);
					System.out.println("user:" + user + " mapper:"
							+ mapper.getName() + " " + run.getName() + " "
							+ expRes.mem() + ":" + expRes.mem_desv() + " "
							+ expRes.net() + ":" + expRes.net_desv() + " "
							+ expRes.time() / 1000 + ":"
							+ expRes.time_desv() / 1000);
				}
			}
		}

		for (GraphlyRun run : groups_exp) {
			for (Mapper mapper : MAPPERS) {
				for (long[] g : GROUPS) {
					ExperimentResult expRes = GraphlyExperiment.exec(reps, g,
							graphName, fact, run, print_results, startNode,
							mapper);
					System.out.println("group:" + Arrays.toString(g)
							+ " mapper:" + mapper.getName() + " "
							+ run.getName() + " " + +expRes.mem() + ":"
							+ expRes.mem_desv() + " " + expRes.net() + ":"
							+ expRes.net_desv() + " " + expRes.time() / 1000
							+ ":" + expRes.time_desv() / 1000);

				}
			}
		}
	}
}
