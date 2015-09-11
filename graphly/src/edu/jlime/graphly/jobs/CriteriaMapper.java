package edu.jlime.graphly.jobs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.metrics.sysinfo.filter.SysInfoFilter;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

public class CriteriaMapper implements Mapper {

	private static final long serialVersionUID = -821812463957389816L;

	private static final int SLOTS = 20001;

	private static final int VNODES = 100;

	private SysInfoFilter<Node> filter;

	private boolean dynamic;

	// private Map<Integer, ClientNode> division = new HashMap<>();
	Node[] division;

	public CriteriaMapper(SysInfoFilter<Node> ext, boolean dynamic) {
		this.filter = ext;
		this.dynamic = dynamic;
	}

	@Override
	public List<Pair<Node, TLongArrayList>> map(int max, long[] data, JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(CriteriaMapper.class);
		HashMap<Node, TLongArrayList> div = new HashMap<Node, TLongArrayList>();

		CompositeMetrics<Node> info = ctx.getCluster().getInfo();

		HashMap<Node, Float> infoValues = filter.extract(info);
		if (log.isDebugEnabled())
			log.debug("Obtained Info for Criteria Mapper  : " + this + " - values " + infoValues);

		// Normalize to [0,1]
		float sum = 0;
		for (Entry<Node, Float> val : infoValues.entrySet()) {
			sum += val.getValue();
		}
		for (Entry<Node, Float> val : infoValues.entrySet()) {
			infoValues.put(val.getKey(), val.getValue() / sum);
		}

		int count = 0;
		int init = 0;
		for (Entry<Node, Float> e : infoValues.entrySet()) {
			count++;
			int end = (int) Math.ceil(init + data.length * e.getValue());
			if (end >= data.length || count == infoValues.size())
				end = data.length;

			long[] dataCopy = Arrays.copyOfRange(data, init, end);
			if (dataCopy.length != 0)
				div.put(e.getKey(), new TLongArrayList(dataCopy));
			init = end;
		}

		return GraphlyUtil.divide(div, max);
	}

	@Override
	public String getName() {
		return "criteria-" + filter.toString();
	}

	@Override
	public boolean isDynamic() {
		return dynamic;
	}

	@Override
	public void update(JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(CriteriaMapper.class);

		CompositeMetrics<Node> info = ctx.getCluster().getInfo();

		HashMap<Node, Float> infoValues = filter.extract(info);
		// if (log.isDebugEnabled())
		log.info("Obtained Info for Criteria Mapper  : " + this + " - values " + infoValues);

		// Normalize sum to [0,1)
		float sum = 0;
		for (Entry<Node, Float> val : infoValues.entrySet()) {
			sum += val.getValue();
		}
		for (Entry<Node, Float> entry : infoValues.entrySet()) {
			entry.setValue(entry.getValue() / sum);
		}

		int acc = 0;
		division = new Node[VNODES];

		Entry<Node, Float>[] array = infoValues.entrySet().toArray(new Entry[] {});
		for (int i = 0; i < array.length; i++) {
			Entry<Node, Float> entry = array[i];
			int to = 0;
			if (i == array.length - 1)
				to = division.length;
			else
				to = (int) (acc + VNODES * entry.getValue());

			for (; acc < to; acc++) {
				division[acc] = entry.getKey();
			}
		}
	}

	@Override
	public Node getNode(long v, JobContext ctx) {
		int hash = (int) (v % VNODES);
		return division[hash];
	}

	@Override
	public Peer[] getPeers() {
		Peer[] peers = new Peer[division.length];
		for (int i = 0; i < division.length; i++) {
			peers[i] = division[i].getPeer();
		}
		return peers;
	}

	@Override
	public int hash(long v, JobContext ctx) {
		return (int) (v % VNODES);
	}
}
