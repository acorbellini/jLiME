package edu.jlime.collections.adjacencygraph.mappers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.JobNode;
import edu.jlime.metrics.sysinfo.filter.SysInfoFilter;
import gnu.trove.list.array.TIntArrayList;

public class CriteriaMapper extends Mapper {

	private static final long serialVersionUID = -821812463957389816L;

	private SysInfoFilter<JobNode> filter;

	public CriteriaMapper(SysInfoFilter<JobNode> ext) {
		this.filter = ext;
	}

	@Override
	public Map<JobNode, TIntArrayList> map(int[] data, JobContext env)
			throws Exception {
		Logger log = Logger.getLogger(CriteriaMapper.class);
		HashMap<JobNode, TIntArrayList> div = new HashMap<JobNode, TIntArrayList>();

		HashMap<JobNode, Float> infoValues = filter.extract(env.getCluster()
				.getInfo());

		log.info("Obtained Info for Criteria Mapper  : " + infoValues);

		// Normalize to [0,1]
		float sum = 0;
		for (Entry<JobNode, Float> val : infoValues.entrySet()) {
			sum += val.getValue();
		}
		for (Entry<JobNode, Float> val : infoValues.entrySet()) {
			infoValues.put(val.getKey(), val.getValue() / sum);
		}

		int count = 0;
		int init = 0;
		for (Entry<JobNode, Float> e : infoValues.entrySet()) {
			count++;
			int end = (int) Math.ceil(init + data.length * e.getValue());
			if (end >= data.length || count == infoValues.size())
				end = data.length;

			int[] dataCopy = Arrays.copyOfRange(data, init, end);
			div.put(e.getKey(), new TIntArrayList(dataCopy));
			init = end;
		}
		return div;
	}
}
