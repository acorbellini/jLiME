package edu.jlime.collections.adjacencygraph.mappers;

import java.util.HashMap;
import java.util.Map;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.JobNode;
import gnu.trove.list.array.TIntArrayList;

public class LocationMapper extends Mapper {

	private static final long serialVersionUID = 1634522852310272015L;

	private String mapName;

	public LocationMapper(String mapName) {
		this.mapName = mapName;
	}

	@Override
	public Map<JobNode, TIntArrayList> map(int[] data, JobContext cluster) {
		PersistentIntIntArrayMap simple = PersistentIntIntArrayMap.getMap(
				mapName, cluster);

		HashMap<JobNode, TIntArrayList> map = simple.getAffinityNode(data);
		return map;
	}

}
