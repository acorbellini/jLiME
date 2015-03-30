package edu.jlime.graphly.jobs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
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
			JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(LocationMapper.class);
		if (log.isDebugEnabled())
			log.debug("Mapping " + data.length + " keys by location.");

		Graphly g = (Graphly) ctx.getGlobal("graphly");

		Map<Peer, TLongArrayList> map = g.getHash().hashKeys(data);
		
		Map<ClientNode, TLongArrayList> ret = new HashMap<>();
		for (Entry<Peer, TLongArrayList> e : map.entrySet()) {
			ret.put(g.getJobClient().getCluster().getClientFor(e.getKey()),
					e.getValue());
		}
		return GraphlyUtil.divide(ret, max);
	}

	@Override
	public String getName() {
		return "location";
	}

	@Override
	public boolean isDynamic() {
		return false;
	}

	@Override
	public void update(JobContext ctx) throws Exception {
	}

	@Override
	public ClientNode getPeer(long v, JobContext ctx) {
		Graphly g = (Graphly) ctx.getGlobal("graphly");

		Peer p = g.getHash().getNode(v);
		
		return g.getJobClient().getCluster().getClientFor(p);
	}
}
