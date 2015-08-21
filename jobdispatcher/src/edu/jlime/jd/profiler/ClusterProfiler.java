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
import edu.jlime.jd.ClientNode;
import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.CSV;

public class ClusterProfiler implements Profiler {

	public static class NodeComparator implements Comparator<ClientNode> {
		@Override
		public int compare(ClientNode o1, ClientNode o2) {
			int comp = o1.getName().compareTo(o2.getName());
			if (comp == 0)
				comp = o1.getPeer().getAddress()
						.compareTo(o2.getPeer().getAddress());
			return comp;
		}
	}

	public static final String SEP = ",";

	long freq;

	TreeMap<Date, CompositeMetrics<ClientNode>> info = new TreeMap<>();

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
						this.cancel();
						return;
					}
					CompositeMetrics<ClientNode> clusterMetrics = c.getInfo();
					// System.out.println(clusterMetrics);
					info.put(Calendar.getInstance().getTime(), clusterMetrics);
				} catch (Exception e) {
					// e.printStackTrace();
				}
			}
		}, 0, freq);
	}

	public void csv(CSV csv, MetricExtractor ext) {
		HashMap<Date, CompositeMetrics<ClientNode>> info = new HashMap<>(
				this.info);

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

	public Map<Date, CompositeMetrics<ClientNode>> getInfo() {
		return info;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.jlime.jd.profiler.Profiler#calcPerNode(edu.jlime.jd.profiler.
	 * ProfilerFunctionPerNode, edu.jlime.jd.profiler.MetricExtractor)
	 */
	@Override
	public <T> Map<ClientNode, T> calcPerNode(
			ProfilerFunctionPerNode<T> profilerFunction, MetricExtractor<T> ext) {
		Map<ClientNode, TreeMap<Date, T>> toCalc = new TreeMap<>(
				new NodeComparator());
		for (Entry<Date, CompositeMetrics<ClientNode>> e : info.entrySet()) {
			CompositeMetrics<ClientNode> composite = e.getValue();
			for (ClientNode clientNode : composite.getKeys()) {
				TreeMap<Date, T> curr = toCalc.get(clientNode);
				if (curr == null) {
					curr = new TreeMap<>();
					toCalc.put(clientNode, curr);
				}
				curr.put(e.getKey(), ext.get(composite.get(clientNode)));
			}
		}
		TreeMap<ClientNode, T> ret = new TreeMap<>(new NodeComparator());
		for (Entry<ClientNode, TreeMap<Date, T>> toCalcEntry : toCalc
				.entrySet()) {
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
			ProfilerFunctionPerDate<T> profilerFunction, MetricExtractor<T> ext) {
		Map<Date, TreeMap<ClientNode, T>> toCalc = new TreeMap<>();

		for (Entry<Date, CompositeMetrics<ClientNode>> e : info.entrySet()) {
			TreeMap<ClientNode, T> curr = toCalc.get(e.getKey());
			if (curr == null) {
				curr = new TreeMap<>(new NodeComparator());
				toCalc.put(e.getKey(), curr);
			}
			CompositeMetrics<ClientNode> composite = e.getValue();
			for (ClientNode clientNode : composite.getKeys()) {
				curr.put(clientNode, ext.get(composite.get(clientNode)));
			}
		}
		Map<Date, T> ret = new TreeMap<>();
		for (Entry<Date, TreeMap<ClientNode, T>> toCalcEntry : toCalc
				.entrySet()) {
			ret.put(toCalcEntry.getKey(),
					profilerFunction.call(toCalcEntry.getValue()));
		}
		return ret;
	}

	public float getNetworkConsumption() {
		Map<ClientNode, Float> diffs = calcPerNode(
				new ProfilerFunctionPerNode<Float>() {

					@Override
					public Float call(TreeMap<Date, Float> value) {
						Float first = Float.valueOf(value.firstEntry()
								.getValue());
						Float last = Float.valueOf(value.lastEntry().getValue());
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
		return netSum;
	}

	public float getMemoryConsumption() {
		Map<Date, Float> memSums = calcPerDate(
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
		return memMax;
	}
}
