package edu.jlime.linkprediction.twitter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import edu.jlime.client.Client;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;
import edu.jlime.collections.adjacencygraph.query.RemoteListQuery;
import edu.jlime.collections.adjacencygraph.query.TopQuery;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.profiler.ClusterProfiler;
import edu.jlime.jd.profiler.MetricExtractor;
import edu.jlime.linkprediction.TwitterStoreConfig;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.CSV;
import edu.jlime.util.CommandLineUtils;
import edu.jlime.util.StringUtils;

public class RecommendationTest {

	private ClientCluster cluster;

	// static String DEF_HOME = "../DEF/dist/def";

	static String DEF_HOME = "/home/acorbellini/jlime";

	StoreConfig config = TwitterStoreConfig.getConfig();

	private RemoteAdjacencyGraph graph;

	public static void main(String[] args) throws Exception {
		FileSystemXmlApplicationContext ctxt = new FileSystemXmlApplicationContext(
				args[0]);
		try {
			Mapper mapper = ctxt.getBean(args[1], Mapper.class);

			if (args.length == 5)
				DEF_HOME = args[4];

			new RecommendationTest().run(mapper, args[1], args[2], new Integer(
					args[3]));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ctxt.close();
		}
	}

	public void run(Mapper mapper, String mapperID, String runID, Integer user)
			throws Exception, FileNotFoundException, IOException {

		// String output = "/home/acorbellini/results/" + mapperID + "/" + user
		// + "/" + user + "-" + mapperID + "-run" + runID + ".log";

		// PrintStream fos = new PrintStream(
		// new FileOutputStream(new File(output)));
		// System.setOut(fos);

		File checkIfExists = new File("/home/acorbellini/results/" + mapperID
				+ "/" + user + "/" + user + "-profile-net-" + mapperID + "-run"
				+ runID + ".csv");
		if (checkIfExists.exists()) {
			System.out.println("Recommendation for user" + user
					+ " already exists.");
			return;
		}

		System.out.println("Running using mapper " + mapper + " with name "
				+ mapperID + " user " + user + " and run " + runID);
		CommandLineUtils.execCommand("bash " + DEF_HOME
				+ "/scripts/clusterrun.sh " + DEF_HOME
				+ "/scripts/eight.txt acorbellini");

		System.out.println("Creating client, waiting for 8 execution nodes.");
		Client client = Client.build(8);

		cluster = client.getCluster();
		System.out.println("Creating adyacency graph.");
		graph = new RemoteAdjacencyGraph(config, cluster, mapper);

		RemoteListQuery followees = graph.getUser(user).followees();

		// RECOMMENDATION ALGORITHM:
		TopQuery query = followees.followers().remove(followees)
				.countFollowees().remove(followees).top(10);

		query.setCacheQuery(false);

		printTop(query, user, mapperID, runID);

		client.close();

		CommandLineUtils.execCommand("bash " + DEF_HOME
				+ "/scripts/clusterstop.sh " + DEF_HOME
				+ "/scripts/eight.txt acorbellini");
		System.exit(0);
	}

	public void printTop(TopQuery query, Integer user, String mapperID,
			String runID) throws Exception {
		System.out.println("Recommendation number " + runID + " to user "
				+ user);
		ClusterProfiler profiler = new ClusterProfiler(cluster, 2000);
		profiler.start();
		Iterator<int[]> it = query.query().iterator();
		while (it.hasNext()) {
			int[] js = (int[]) it.next();
			System.out.println(js[0] + " ==> " + js[1]);
		}
		profiler.stop();
		System.out.println("Query Time:  "
				+ StringUtils.readableTime(query.getQueryTime()));

		System.out.println("Closing graph.");
		graph.close();

		Thread.sleep(1000);

		System.out.println("Saving profile information.");

		CSV writer = CSV.fileCSV("/home/acorbellini/results/" + mapperID + "/"
				+ user + "/" + user + "-" + mapperID + ".csv", true, "UserID",
				"Mapper", "Run", "Query Time");
		writer.putLine(user.toString(), mapperID, runID, query.getQueryTime()
				.toString());
		writer.close();

		profiler.csv(
				CSV.fileCSV("/home/acorbellini/results/" + mapperID + "/"
						+ user + "/" + user + "-profile-net-" + mapperID
						+ "-run" + runID + ".csv", true),
				new MetricExtractor() {
					@Override
					public String get(Metrics m) {

						return m.list("sysinfo.net").findFirst("eth")
								.get("sent_total").get();
					}
				});

		profiler.csv(
				CSV.fileCSV("/home/acorbellini/results/" + mapperID + "/"
						+ user + "/" + user + "-profile-mem-" + mapperID
						+ "-run" + runID + ".csv", true),
				new MetricExtractor() {

					@Override
					public String get(Metrics m) {
						return m.get("jvminfo.mem.used").get();
					}
				});
	}
}
