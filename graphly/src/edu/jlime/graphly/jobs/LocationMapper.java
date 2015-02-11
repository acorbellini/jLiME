package edu.jlime.graphly.jobs;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import gnu.trove.list.array.TLongArrayList;

public class LocationMapper extends Mapper {

	private static final long serialVersionUID = 1634522852310272015L;

	@Override
	public Map<ClientNode, TLongArrayList> map(long[] data, JobContext cluster)
			throws Exception {
		Logger log = Logger.getLogger(LocationMapper.class);

		log.info("Mapping " + data.length + " keys by location.");

		Graphly g = (Graphly) cluster.getGlobal("graphly");

		Map<GraphlyStoreNodeI, TLongArrayList> map = g.hashKeys(data);

		Map<ClientNode, TLongArrayList> ret = new HashMap<>();
		for (Entry<GraphlyStoreNodeI, TLongArrayList> e : map.entrySet()) {
			ret.put(g.getClientJobFor(e.getKey()), e.getValue());
		}

		log.info("Finished mapping " + data.length + " keys by location.");

		return ret;
	}

}
