package edu.jlime.jd.profiler;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.util.CSV;

public class ClusterProfiler {

	public static final String SEP = ",";

	long freq;

	HashMap<Date, CompositeMetrics<ClientNode>> info = new HashMap<>();

	Timer timer;

	private ClientCluster c;

	public ClusterProfiler(ClientCluster c, long freq) {
		super();
		this.freq = freq;
		this.c = c;
	}

	public void start() {
		timer = new Timer("Cluster Profiler", true);
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					CompositeMetrics<ClientNode> clusterMetrics = c.getInfo();
//					System.out.println(clusterMetrics);
					info.put(Calendar.getInstance().getTime(), clusterMetrics);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, freq, freq);
	}

	public void csv(CSV csv, MetricExtractor ext) {
		HashMap<Date, CompositeMetrics<ClientNode>> info = new HashMap<>(this.info);

		HashSet<ClientNode> peersUsed = new HashSet<>();
		for (Date ci : info.keySet()) {
			peersUsed.addAll(info.get(ci).getKeys());
		}
		List<ClientNode> sorted = new ArrayList<>(peersUsed);

		Collections.sort(sorted, new Comparator<ClientNode>() {

			@Override
			public int compare(ClientNode o1, ClientNode o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});

		csv.put("Time/Node");
		for (ClientNode peer : sorted) {
			csv.put(peer.getName());
		}
		csv.newLine();

		List<Date> dates = new ArrayList<>(info.keySet());
		Collections.sort(dates);
		for (Date date : dates) {
			csv.put(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
			for (ClientNode peer : sorted) {
				CompositeMetrics<ClientNode> i = info.get(date);
				if (i.contains(peer))
					csv.put(ext.get(i.get(peer)));

				else
					csv.put("");
			}
			csv.newLine();
		}
		try {
			csv.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		timer.cancel();
	}

	public static void main(String[] args) {
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
				.format(Calendar.getInstance().getTime()));
	}

	public String print(MetricExtractor ext) {
		StringWriter writer = new StringWriter();
		csv(new CSV(writer), ext);
		return writer.toString();
	}
}
