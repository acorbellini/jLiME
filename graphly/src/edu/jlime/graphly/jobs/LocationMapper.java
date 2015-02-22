package edu.jlime.graphly.jobs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.graphly.util.Pair;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import gnu.trove.list.array.TLongArrayList;

public class LocationMapper implements Mapper {

	private static final long serialVersionUID = 1634522852310272015L;

	@Override
	public List<Pair<ClientNode, TLongArrayList>> map(int max, long[] data,
			JobContext cluster) throws Exception {
		Logger log = Logger.getLogger(LocationMapper.class);
		if (log.isDebugEnabled())
			log.debug("Mapping " + data.length + " keys by location.");

		Graphly g = (Graphly) cluster.getGlobal("graphly");

		Map<GraphlyStoreNodeI, TLongArrayList> map = g.hashKeys(data);

		Map<ClientNode, TLongArrayList> ret = new HashMap<>();
		for (Entry<GraphlyStoreNodeI, TLongArrayList> e : map.entrySet()) {
			ret.put(g.getClientJobFor(e.getKey()), e.getValue());
		}
		if (log.isDebugEnabled())
			log.debug("Finished mapping " + data.length + " keys by location.");

		return GraphlyUtil.divide(ret, max);
	}

	@Override
	public String getName() {
		return "location";
	}
}
