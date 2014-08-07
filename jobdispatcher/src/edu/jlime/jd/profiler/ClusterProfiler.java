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

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.util.CSV;

public class ClusterProfiler {

	public static final String SEP = ",";

	long freq;

	HashMap<Date, CompositeMetrics> info = new HashMap<>();

	Timer timer;

	private JobCluster c;

	public ClusterProfiler(JobCluster c, long freq) {
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
					info.put(Calendar.getInstance().getTime(), c.getInfo());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, freq, freq);
	}

	public void csv(CSV csv, MetricExtractor ext) {
		HashMap<Date, CompositeMetrics> info = new HashMap<>(this.info);

		HashSet<JobNode> peersUsed = new HashSet<>();
		for (Date ci : info.keySet()) {
			peersUsed.addAll(info.get(ci).getKeys());
		}
		List<JobNode> sorted = new ArrayList<>(peersUsed);

		Collections.sort(sorted, new Comparator<JobNode>() {

			@Override
			public int compare(JobNode o1, JobNode o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});

		csv.put("Time/Node");
		for (JobNode peer : sorted) {
			csv.put(peer.getName());
		}
		csv.newLine();

		List<Date> dates = new ArrayList<>(info.keySet());
		Collections.sort(dates);
		for (Date date : dates) {
			csv.put(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
			for (JobNode peer : sorted) {
				CompositeMetrics i = info.get(date);
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
