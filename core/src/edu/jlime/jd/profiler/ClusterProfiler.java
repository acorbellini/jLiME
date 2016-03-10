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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.Node;
import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.MetricListItem;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.CSV;

public class ClusterProfiler implements Profiler {

	public static class NodeComparator implements Comparator<Node> {
		@Override
		public int compare(Node o1, Node o2) {
			int comp = o1.getName().compareTo(o2.getName());
			if (comp == 0)
				comp = o1.getPeer().getAddress()
						.compareTo(o2.getPeer().getAddress());
			return comp;
		}
	}

	public static final MetricExtractor<Float> NET_EXTRACTOR = new MetricExtractor<Float>() {
		@Override
		public Float get(Metrics m) {
			MetricListItem findFirst = m.list("sysinfo.net")
					.findFirst("eth|p7p1|p4p1");
			Metric<?> metric = findFirst.get("sent_total");
			return Float.valueOf(metric.get());
		}
	};

	public static final MetricExtractor<Float> USED_MEM_EXTRACTOR = new MetricExtractor<Float>() {

		@Override
		public Float get(Metrics m) {
			return Float.valueOf(m.get("jvminfo.mem.used").get());
		}
	};

	public static final String SEP = ",";

	long freq;

	TreeMap<Date, CompositeMetrics<Node>> info = new TreeMap<>();

	Timer timer;

	private ClientCluster c;

	private volatile boolean stopTimer;

	private volatile boolean timer_stopped;

	public ClusterProfiler(ClientCluster c, long freq) {
		super();
		this.freq = freq;
		this.c = c;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.jd.profiler.Profiler#start()
	 */
	@Override
	public void start() {
		timer = new Timer("Cluster Profiler", true);
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					if (stopTimer) {
						timer_stopped = true;
						synchronized (timer) {
							timer.notifyAll();
						}
						timer.cancel();
						return;
					}
					CompositeMetrics<Node> clusterMetrics = c.getInfo();
					// System.out.println(clusterMetrics);
					info.put(Calendar.getInstance().getTime(), clusterMetrics);
				} catch (Exception e) {
					// e.printStackTrace();
				}
			}
		}, 0, freq);
	}

	public void csv(CSV csv, MetricExtractor ext) {
		HashMap<Date, CompositeMetrics<Node>> info = new HashMap<>(this.info);

		HashSet<Node> peersUsed = new HashSet<>();
		for (Date ci : info.keySet()) {
			peersUsed.addAll(info.get(ci).getKeys());
		}
		List<Node> sorted = new ArrayList<>(peersUsed);

		Collections.sort(sorted, new Comparator<Node>() {

			@Override
			public int compare(Node o1, Node o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});

		csv.put("Time/Node");
		for (Node peer : sorted) {
			csv.put(peer.getName());
		}
		csv.newLine();

		List<Date> dates = new ArrayList<>(info.keySet());
		Collections.sort(dates);
		for (Date date : dates) {
			csv.put(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
			for (Node peer : sorted) {
				CompositeMetrics<Node> i = info.get(date);
				if (i.contains(peer))
					csv.put(ext.get(i.get(peer)).toString());

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.jd.profiler.Profiler#stop()
	 */
	@Override
	public void stop() {
		stopTimer = true;
		synchronized (timer) {
			while (!timer_stopped) {
				try {
					timer.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
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

	public Map<Date, CompositeMetrics<Node>> getInfo() {
		return info;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.jd.profiler.Profiler#calcPerNode(edu.jlime.jd.profiler.
	 * ProfilerFunctionPerNode, edu.jlime.jd.profiler.MetricExtractor)
	 */
	@Override
	public <T> Map<Node, T> calcPerNode(
			ProfilerFunctionPerNode<T> profilerFunction,
			MetricExtractor<T> ext) {
		Map<Node, TreeMap<Date, T>> toCalc = new TreeMap<>(
				new NodeComparator());
		for (Entry<Date, CompositeMetrics<Node>> e : info.entrySet()) {
			CompositeMetrics<Node> composite = e.getValue();
			for (Node clientNode : composite.getKeys()) {
				TreeMap<Date, T> curr = toCalc.get(clientNode);
				if (curr == null) {
					curr = new TreeMap<>();
					toCalc.put(clientNode, curr);
				}
				curr.put(e.getKey(), ext.get(composite.get(clientNode)));
			}
		}
		TreeMap<Node, T> ret = new TreeMap<>(new NodeComparator());
		for (Entry<Node, TreeMap<Date, T>> toCalcEntry : toCalc.entrySet()) {
			ret.put(toCalcEntry.getKey(),
					profilerFunction.call(toCalcEntry.getValue()));
		}
		return ret;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.jd.profiler.Profiler#calcPerDate(edu.jlime.jd.profiler.
	 * ProfilerFunctionPerDate, edu.jlime.jd.profiler.MetricExtractor)
	 */
	@Override
	public <T> Map<Date, T> calcPerDate(
			ProfilerFunctionPerDate<T> profilerFunction,
			MetricExtractor<T> ext) {
		Map<Date, TreeMap<Node, T>> toCalc = new TreeMap<>();

		for (Entry<Date, CompositeMetrics<Node>> e : info.entrySet()) {
			TreeMap<Node, T> curr = toCalc.get(e.getKey());
			if (curr == null) {
				curr = new TreeMap<>(new NodeComparator());
				toCalc.put(e.getKey(), curr);
			}
			CompositeMetrics<Node> composite = e.getValue();
			for (Node clientNode : composite.getKeys()) {
				curr.put(clientNode, ext.get(composite.get(clientNode)));
			}
		}
		Map<Date, T> ret = new TreeMap<>();
		for (Entry<Date, TreeMap<Node, T>> toCalcEntry : toCalc.entrySet()) {
			ret.put(toCalcEntry.getKey(),
					profilerFunction.call(toCalcEntry.getValue()));
		}
		return ret;
	}

	public float getNetworkConsumption() {

		Map<Node, Float> diffs = calcPerNode(
				new ProfilerFunctionPerNode<Float>() {

					@Override
					public Float call(TreeMap<Date, Float> value) {
						Float first = Float
								.valueOf(value.firstEntry().getValue());
						Float last = Float
								.valueOf(value.lastEntry().getValue());
						return last - first;
					}
				}, NET_EXTRACTOR);
		float netSum = 0f;
		for (Entry<Node, Float> netentry : diffs.entrySet()) {
			netSum += netentry.getValue();
		}
		return netSum;
	}

	public float getMemoryConsumption() {

		Map<Date, Float> memSums = calcPerDate(
				new ProfilerFunctionPerDate<Float>() {

					@Override
					public Float call(TreeMap<Node, Float> value) {
						float sum = 0f;
						for (Entry<Node, Float> e : value.entrySet()) {
							sum += Float.valueOf(e.getValue());
						}
						return sum;
					}
				}, USED_MEM_EXTRACTOR);

		float memMax = 0f;
		for (Entry<Date, Float> memEntry : memSums.entrySet()) {
			if (memEntry.getValue() > memMax)
				memMax = memEntry.getValue();
		}
		return memMax;
	}
}
