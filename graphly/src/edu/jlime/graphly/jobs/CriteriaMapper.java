package edu.jlime.graphly.jobs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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

	private static final int PRIME = 1299709;

	private static final long serialVersionUID = -821812463957389816L;

	private static final int V_BUCKETS = 100;

	Mapper location = MapperFactory.location();

	private SysInfoFilter<Node> filter;

	private boolean dynamic;

	Map<Node, Node[]> probs;

	Map<Node, Integer> pos;

	Node[] nodes;

	public CriteriaMapper(SysInfoFilter<Node> ext, boolean dynamic) {
		this.filter = ext;
		this.dynamic = dynamic;
	}

	@Override
	public List<Pair<Node, TLongArrayList>> map(int max, long[] data,
			JobContext ctx) throws Exception {
		HashMap<Node, TLongArrayList> div = new HashMap<Node, TLongArrayList>();
		update(ctx);

		for (long l : data) {
			Node n = getNode(l, ctx);
			TLongArrayList list = div.get(n);
			if (list == null) {
				list = new TLongArrayList();
				div.put(n, list);
			}
			list.add(l);
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

		location.update(ctx);

		CompositeMetrics<Node> info = ctx.getCluster().getInfo();

		HashMap<Node, Float> infoValues = filter.extract(info);
		// if (log.isDebugEnabled())
		log.info("Obtained Info for Criteria Mapper  : " + this + " - values "
				+ infoValues);

		// Normalize sum to [0,1)
		float max = Float.MIN_VALUE;
		for (Entry<Node, Float> e : infoValues.entrySet()) {
			float val = e.getValue();
			if (val > max)
				max = val;
		}

		float sum = 0;
		for (Entry<Node, Float> entry : infoValues.entrySet()) {
			float value = (float) entry.getValue() / max;
			entry.setValue(value);
			sum += value;
		}

		nodes = infoValues.keySet().toArray(new Node[] {});
		probs = new HashMap<>();
		pos = new HashMap<>();

		for (int i = 0; i < nodes.length; i++)
			pos.put(nodes[i], i);

		nodes = infoValues.keySet().toArray(new Node[] {});
		for (int j = 0; j < nodes.length; j++) {
			float prob = infoValues.get(nodes[j]);
			float diff = 1 - prob;
			Node[] probsNode = probs.get(nodes[j]);
			if (probsNode == null) {
				probsNode = new Node[V_BUCKETS];
				probs.put(nodes[j], probsNode);
			}

			// Calculo probs para el actual
			float[] probs = new float[nodes.length];
			for (int i = 0; i < nodes.length; i++) {
				if (i == j)
					probs[i] = prob;
				else if (diff > 0)
					probs[i] = (infoValues.get(nodes[i]) / (sum - prob)) * diff;
			}

			int acc = 0;
			for (int i = 0; i < probs.length; i++) {
				float probability = probs[i];
				Node n = nodes[i];
				int to = 0;
				if (i == probs.length - 1)
					to = probsNode.length;
				else
					to = (int) (acc + V_BUCKETS * probability);

				for (; acc < to; acc++) {
					probsNode[acc] = n;
				}
			}
		}
		StringBuilder builder = new StringBuilder();
		for (Entry<Node, Node[]> e : probs.entrySet()) {
			builder.append(e.getKey().getName() + ": [");
			Node curr = null;
			int cont = 1;
			for (int i = 0; i < e.getValue().length; i++) {
				Node node = e.getValue()[i];

				if (curr == null)
					curr = node;

				if (curr != node || i == e.getValue().length - 1) {
					builder.append(curr.getName() + ":" + cont + ",");
					curr = node;
					cont = 1;
				} else
					cont++;

			}
			builder.append("]\n");
		}
		System.out.println(builder.toString() + "\n");

	}

	@Override
	public Node getNode(long v, JobContext ctx) {
		Node byLoc = location.getNode(v, ctx);
		return probs.get(byLoc)[(int) ((v * PRIME) % V_BUCKETS)];
	}

	@Override
	public Peer[] getPeers() {
		Peer[] peers = new Peer[nodes.length];
		for (int i = 0; i < nodes.length; i++) {
			peers[i] = nodes[i].getPeer();
		}
		return peers;
	}

	public static void main(String[] args) {
		Map<String, Float> infoValues = new TreeMap<String, Float>();
		infoValues.put("Node0", 5000f);
		infoValues.put("Node1", 3500f);
		infoValues.put("Node2", 500f);
		infoValues.put("Node3", 250f);
		infoValues.put("Node4", 100f);

		float max = Float.MIN_VALUE;
		for (Entry<String, Float> val : infoValues.entrySet()) {
			if (val.getValue() > max)
				max = val.getValue();
		}

		float sum = 0;

		for (Entry<String, Float> entry : infoValues.entrySet()) {
			float value = entry.getValue() / max;
			entry.setValue(value);
			sum += value;
		}

		float[][] probs = new float[infoValues.size()][];
		String[] nodes = infoValues.keySet().toArray(new String[] {});
		for (int j = 0; j < nodes.length; j++) {
			float prob = infoValues.get(nodes[j]);
			float diff = 1 - prob;
			probs[j] = new float[nodes.length];
			for (int i = 0; i < nodes.length; i++) {
				if (i == j)
					probs[j][i] = prob;
				else if (diff > 0) {
					probs[j][i] = (infoValues.get(nodes[i]) / (sum - prob))
							* diff;
				}
			}
		}

		for (float[] fs : probs) {
			System.out.println(Arrays.toString(fs));
			float sumF = 0;
			for (float f : fs) {
				sumF += f;
			}
			System.out.println(sumF);
		}
	}

	@Override
	public int hash(long v, JobContext ctx) {
		Node byLoc = location.getNode(v, ctx);
		Node n = probs.get(byLoc)[(int) ((v * PRIME) % V_BUCKETS)];
		return pos.get(n);
	}
}
