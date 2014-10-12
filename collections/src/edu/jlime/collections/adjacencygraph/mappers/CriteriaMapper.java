package edu.jlime.collections.adjacencygraph.mappers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.metrics.sysinfo.filter.SysInfoFilter;
import gnu.trove.list.array.TIntArrayList;

public class CriteriaMapper extends Mapper {

	private static final long serialVersionUID = -821812463957389816L;

	private SysInfoFilter<ClientNode> filter;

	public CriteriaMapper(SysInfoFilter<ClientNode> ext) {
		this.filter = ext;
	}

	@Override
	public Map<ClientNode, TIntArrayList> map(int[] data, JobContext env)
			throws Exception {
		Logger log = Logger.getLogger(CriteriaMapper.class);
		HashMap<ClientNode, TIntArrayList> div = new HashMap<ClientNode, TIntArrayList>();

		CompositeMetrics<ClientNode> info = env.getCluster().getInfo();

		HashMap<ClientNode, Float> infoValues = filter.extract(info);

		log.info("Obtained Info for Criteria Mapper  : " + this + " - values "
				+ infoValues);

		// Normalize to [0,1]
		float sum = 0;
		for (Entry<ClientNode, Float> val : infoValues.entrySet()) {
			sum += val.getValue();
		}
		for (Entry<ClientNode, Float> val : infoValues.entrySet()) {
			infoValues.put(val.getKey(), val.getValue() / sum);
		}

		int count = 0;
		int init = 0;
		for (Entry<ClientNode, Float> e : infoValues.entrySet()) {
			count++;
			int end = (int) Math.ceil(init + data.length * e.getValue());
			if (end >= data.length || count == infoValues.size())
				end = data.length;

			int[] dataCopy = Arrays.copyOfRange(data, init, end);
			if (dataCopy.length != 0)
				div.put(e.getKey(), new TIntArrayList(dataCopy));
			init = end;
		}

		for (Entry<ClientNode, TIntArrayList> e : div.entrySet()) {
			log.info(e.getKey() + " -> " + e.getValue().size());
		}

		return div;
	}
}
