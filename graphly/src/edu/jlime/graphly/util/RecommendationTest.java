package edu.jlime.graphly.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.server.GraphlyServer;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.profiler.ClusterProfiler;
import edu.jlime.jd.profiler.MetricExtractor;
import edu.jlime.jd.profiler.ProfilerFunctionPerDate;
import edu.jlime.jd.profiler.ProfilerFunctionPerNode;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.CommandLineUtils;
import edu.jlime.util.StringUtils;
import edu.jlime.util.table.CSVBuilder;
import edu.jlime.util.table.Table;
import gnu.trove.list.array.TLongArrayList;

public class RecommendationTest {

	private String resultsDir;

	private String xmlconf;

	private ArrayList<Mapper> mappers;

	private Integer servers;

	private Integer runs;

	private FileSystemXmlApplicationContext ctxt;

	private ArrayList<QueryContainer> queries;

	private Map<String, String> users;

	private String execType;

	private List<GraphlyServer> localServers;

	private String graphID;

	public RecommendationTest(String results, String config) {
		this.resultsDir = results;
		this.xmlconf = config;

		this.ctxt = new FileSystemXmlApplicationContext(xmlconf);

		this.graphID = ctxt.getBean("graphID", String.class);

		this.mappers = ctxt.getBean("mappers", ArrayList.class);

		this.servers = ctxt.getBean("servers", Integer.class);

		this.execType = ctxt.getBean("executiontype", String.class);

		this.runs = ctxt.getBean("runs", Integer.class);

		this.queries = ctxt.getBean("queries", ArrayList.class);

		this.users = ctxt.getBean("usergroups", Map.class);
	}

	public static void main(String[] args) throws Exception {

		new RecommendationTest(args[0], args[1]).execute();

	}

	private void execute() throws Exception {
		String timeFile = resultsDir + "/time.csv";
		String netFile = resultsDir + "/network.csv";
		String memFile = resultsDir + "/memory.csv";

		Table timeTable = new CSVBuilder(timeFile).toTable();
		Table networkTable = new CSVBuilder(netFile).toTable();
		Table memoryTable = new CSVBuilder(memFile).toTable();

		for (QueryContainer q : queries) {

			String queryID = q.getID();

			for (Mapper mapper : mappers) {
				// Mapper mapper = ctxt.getBean(mapperName, Mapper.class);
				for (Entry<String, String> ug : users.entrySet())
					for (int runID = 0; runID < Integer.valueOf(runs); runID++) {
						if (timeTable.find(queryID, runID, ug.getKey(),
								mapper.getName())) {
							System.out.println("Running Experiment " + queryID
									+ " run " + runID + " mapper "
									+ mapper.getName() + " users "
									+ ug.getKey());
						} else {
							System.out.println("Running Experiment " + queryID
									+ " run " + runID + " mapper "
									+ mapper.getName() + " users "
									+ ug.getKey());
							String[] split = ug.getValue().trim()
									.replaceAll("\\s+", " ").split(" ");
							TLongArrayList list = new TLongArrayList();
							for (String string : split) {
								list.add(Long.valueOf(string));
							}
							run(mapper, mapper.getName(), runID, queryID, q,
									ug.getKey(), list.toArray(), timeTable,
									networkTable, memoryTable);
							timeTable.toCSV(timeFile);
							networkTable.toCSV(netFile);
							memoryTable.toCSV(memFile);
						}
					}
			}
		}
		ctxt.close();

	}

	public void run(Mapper mapper, String mapperName, Integer runID,
			String experiment, QueryContainer queryContainer, String ug,
			long[] users, Table time, Table net, Table mem) throws Exception,
			FileNotFoundException, IOException {
		if (execType.equals("cluster"))
			CommandLineUtils
					.execCommand("bash graphly.sh start eight.txt acorbellini");
		else {
			String localdir = ctxt.getBean("localdir", String.class);
			this.localServers = GraphlyServer.createServers(localdir,
					this.servers, true, this.servers);
		}
		System.out.println("Creating client, waiting for " + servers
				+ " execution nodes.");

		Graphly graph = Graphly.build(servers);
		GraphlyGraph g = graph.getGraph(graphID);
		ClusterProfiler profiler = new ClusterProfiler(graph.getJobClient()
				.getCluster(), 2000);
		profiler.start();
		long init = System.currentTimeMillis();
		queryContainer.run(g, users, mapper);
		long total = System.currentTimeMillis() - init;

		System.out.println("Query Time:  " + StringUtils.readableTime(total));

		profiler.stop();

		graph.close();

		System.out.println("Saving profile information.");

		time.addRow(experiment, runID, ug, mapper.getName(), total / 1000);

		Map<ClientNode, Float> diffs = profiler.calcPerNode(
				new ProfilerFunctionPerNode<Float>() {

					@Override
					public Float call(TreeMap<Date, Float> value) {
						Float first = Float.valueOf(value.firstEntry()
								.getValue());
						Float last = Float
								.valueOf(value.lastEntry().getValue());
						return last - first;
					}
				}, new MetricExtractor<Float>() {
					@Override
					public Float get(Metrics m) {
						return Float.valueOf(m.list("sysinfo.net")
								.findFirst("eth|p7p1").get("sent_total").get());
					}
				});
		float netSum = 0f;
		for (Entry<ClientNode, Float> netentry : diffs.entrySet()) {
			netSum += netentry.getValue();
		}

		net.addRow(experiment, runID, ug, mapper.getName(), netSum);

		Map<Date, Float> memSums = profiler.calcPerDate(
				new ProfilerFunctionPerDate<Float>() {

					@Override
					public Float call(TreeMap<ClientNode, Float> value) {
						float sum = 0f;
						for (Entry<ClientNode, Float> e : value.entrySet()) {
							sum += Float.valueOf(e.getValue());
						}
						return sum;
					}
				}, new MetricExtractor<Float>() {

					@Override
					public Float get(Metrics m) {
						return Float.valueOf(m.get("jvminfo.mem.used").get());
					}
				});

		float memMax = 0f;
		for (Entry<Date, Float> memEntry : memSums.entrySet()) {
			if (memEntry.getValue() > memMax)
				memMax = memEntry.getValue();
		}
		mem.addRow(experiment, runID, ug, mapper.getName(), memMax);
		if (execType.equals("cluster"))
			CommandLineUtils
					.execCommand("bash graphly.sh stop eight.txt acorbellini");
		else {
			for (GraphlyServer l : this.localServers) {
				l.stop();
			}
		}
	}
}
