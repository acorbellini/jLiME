package edu.jlime.graphly.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.server.GraphlyServer;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.jd.profiler.ClusterProfiler;

public class GraphlyExperiment {

	public class ExperimentResult {

		public class Experiment {
			long time;
			float net;
			float mem;

			public Experiment(long time, float net, float mem) {
				this.time = time;
				this.net = net;
				this.mem = mem;
			}

		}

		List<Experiment> exps = new ArrayList<>();

		public void addExperiment(long time, float net, float mem) {
			exps.add(new Experiment(time, net, mem));
		}

		public float time() {
			float prom = 0;
			for (Experiment e : exps) {
				prom += e.time;
			}
			return prom / (float) exps.size();
		}

		public float net() {
			float prom = 0;
			for (Experiment e : exps) {
				prom += e.net;
			}
			return prom / (float) exps.size();
		}

		public float mem() {
			float prom = 0;
			for (Experiment e : exps) {
				prom += e.mem;
			}
			return prom / (float) exps.size();
		}

		public float time_desv() {
			float prom = 0;
			float prom_2 = 0;
			for (Experiment e : exps) {
				prom += e.time;
				prom_2 += e.time * e.time;
			}
			prom = prom / exps.size();
			return (float) Math.sqrt(prom_2 / exps.size() - prom * prom);
		}

		public float net_desv() {
			double prom = 0;
			for (Experiment e : exps) {
				prom += e.net / exps.size();
			}

			double prom_2 = 0;
			for (Experiment e : exps) {
				prom_2 += (e.net - prom) * (e.net - prom);
			}

			return (float) Math.sqrt(prom_2 / exps.size());
		}

		public float mem_desv() {
			float prom = 0;
			float prom_2 = 0;
			for (Experiment e : exps) {
				prom += e.mem;
				prom_2 += e.mem * e.mem;
			}
			prom = prom / exps.size();
			return (float) Math.sqrt(prom_2 / exps.size() - prom * prom);
		}

	}

	Logger log = Logger.getLogger(GraphlyExperiment.class);

	private String graphName;
	private String start;

	private GraphlyRun run;
	private GraphlyServerFactory fact;

	private boolean print_res;
	private long[] users;

	private int reps;

	private Mapper mapper;

	public GraphlyExperiment(int reps, long[] users, GraphlyRun run,
			GraphlyServerFactory fact, String graph, boolean print_results,
			String startNode, Mapper mapper) {
		this.reps = reps;
		this.run = run;
		this.fact = fact;
		this.graphName = graph;
		this.print_res = print_results;
		this.start = startNode;
		this.users = users;
		this.mapper = mapper;
	}

	public ExperimentResult execute() throws Exception {
		ExperimentResult expRes = new ExperimentResult();
		// prof.getNetworkConsumption(), prof.getMemoryConsumption(),
		// (System.currentTimeMillis() - start));

		for (int i = 0; i < reps + 1; i++) {

			long start = System.currentTimeMillis();
			GraphlyServer server = fact.build();
			server.start();

			GraphlyClient graphly = server.getGraphlyClient();

			GraphlyGraph graph = graphly.getGraph(graphName);

			ClusterProfiler prof = new ClusterProfiler(
					graphly.getJobClient().getCluster(), 1000);
			prof.start();
			GraphlyTraversal tr = run.run(users, graph, mapper);
			TraversalResult res = null;
			if (this.start != null)
				res = tr.submit(graphly.getJobClient().getCluster()
						.getByName(this.start));
			else
				res = tr.exec();
			long time = System.currentTimeMillis() - start;
			prof.stop();
			if (print_res)
				System.out.println(run.printResult(res, graph));

			if (i > 0) {
				expRes.addExperiment(time, prof.getNetworkConsumption(),
						prof.getMemoryConsumption());
			}
			graphly.close();
			server.stop();
		}
		return expRes;

	}

	public static ExperimentResult exec(int reps, long[] users, String graph,
			GraphlyServerFactory fact, GraphlyRun run, boolean print_results,
			String startNode, Mapper mapper) throws Exception {
		return new GraphlyExperiment(reps, users, run, fact, graph,
				print_results, startNode, mapper).execute();

	}
}
