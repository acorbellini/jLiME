package edu.jlime.graphly.util;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mina.proxy.utils.StringUtilities;

import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.server.GraphlyServer;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.profiler.ClusterProfiler;
import edu.jlime.jd.profiler.MetricExtractor;
import edu.jlime.jd.profiler.ProfilerFunctionPerNode;
import edu.jlime.metrics.metric.Metrics;

public class GraphlyExperiment {

	private String name;
	private String graphName;
	private GraphlyRun run;
	private GraphlyServerFactory fact;
	Logger log = Logger.getLogger(GraphlyExperiment.class);

	public GraphlyExperiment(String name, GraphlyRun run,
			GraphlyServerFactory fact, String graph) {
		this.name = name;
		this.run = run;
		this.fact = fact;
		this.graphName = graph;
	}

	public void execute() throws Exception {
		long start = System.currentTimeMillis();
		GraphlyServer server = fact.build();
		server.start();

		GraphlyClient graphly = server.getGraphlyClient();

		GraphlyGraph graph = graphly.getGraph(graphName);

		ClusterProfiler prof = new ClusterProfiler(graphly.getJobClient()
				.getCluster(), 1000);
		prof.start();
		run.run(graph);
		prof.stop();
		System.out.println(name + StringUtils.repeat(" ", 24 - name.length())
				+ prof.getNetworkConsumption() + "\t"
				+ prof.getMemoryConsumption() + "\t"
				+ (System.currentTimeMillis() - start) / 1000);

		graphly.close();
		server.stop();

	}

}
