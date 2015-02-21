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
import edu.jlime.graphly.jobs.Mapper;
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

public class RecommendationTest {

	// private ClientCluster cluster;

	private Graphly graph;

	private String resultsDir;

	private String xmlconf;

	private String[] mappers;

	private Integer servers;

	private Integer runs;

	private FileSystemXmlApplicationContext ctxt;

	public RecommendationTest(String results, String config) {
		this.resultsDir = results;
		this.xmlconf = config;

		this.ctxt = new FileSystemXmlApplicationContext(xmlconf);

		this.mappers = ctxt.getBean("mappers", String[].class);

		this.servers = ctxt.getBean("servers", Integer.class);

		this.runs = ctxt.getBean("runs", Integer.class);
	}

	public static void main(String[] args) throws Exception {

		new RecommendationTest(args[0], args[1]).execute();

	}

	private void execute() throws Exception {

		List<QueryContainer> queries = new ArrayList<>();
		for (QueryContainer q : queries) {

			String queryID = q.getID();
			String timeFile = resultsDir + "/" + queryID + "-time.csv";
			String netFile = resultsDir + "/" + queryID + "-network.csv";
			String memFile = resultsDir + "/" + queryID + "-memory.csv";

			Table timeTable = new CSVBuilder(timeFile).toTable();
			Table networkTable = new CSVBuilder(netFile).toTable();
			Table memoryTable = new CSVBuilder(memFile).toTable();

			for (String mapperName : mappers) {
				Mapper mapper = ctxt.getBean(mapperName, Mapper.class);

				for (int runID = 0; runID < Integer.valueOf(runs); runID++) {
					if (timeTable.find(queryID, runID)) {
						System.out.println("Experiment " + queryID
								+ " and run " + runID + " already exists.");
					} else {
						run(mapper, mapperName, runID, queryID, q, timeTable,
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
			String experiment, QueryContainer queryContainer, Table time,
			Table net, Table mem) throws Exception, FileNotFoundException,
			IOException {

		System.out.println("Running experiment " + experiment);

		CommandLineUtils
				.execCommand("bash graphly.sh start eight.txt acorbellini");

		System.out.println("Creating client, waiting for " + servers
				+ " execution nodes.");

		graph = Graphly.build(servers);

		ClusterProfiler profiler = new ClusterProfiler(graph.getJobClient()
				.getCluster(), 2000);
		profiler.start();

		long init = System.currentTimeMillis();
		queryContainer.run();
		long total = System.currentTimeMillis() - init;

		System.out.println("Query Time:  " + StringUtils.readableTime(total));

		profiler.stop();

		graph.close();

		System.out.println("Saving profile information.");

		time.addRow(experiment, runID, mapper, total / 1000);

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
								.findFirst("eth").get("sent_total").get());
					}
				});
		float netSum = 0f;
		for (Entry<ClientNode, Float> netentry : diffs.entrySet()) {
			netSum += netentry.getValue();
		}

		net.addRow(experiment, runID, mapper, netSum);

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
		mem.addRow(experiment, runID, mapper, memMax);

		CommandLineUtils
				.execCommand("bash graphly.sh stop eight.txt acorbellini");
	}
}
