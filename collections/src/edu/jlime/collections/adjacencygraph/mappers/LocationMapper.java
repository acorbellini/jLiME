package edu.jlime.collections.adjacencygraph.mappers;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import gnu.trove.list.array.TIntArrayList;

public class LocationMapper extends Mapper {

	private static final long serialVersionUID = 1634522852310272015L;

	private String mapName;

	public LocationMapper(String mapName) {
		this.mapName = mapName;
	}

	@Override
	public Map<ClientNode, TIntArrayList> map(int[] data, JobContext cluster) {

		Logger log = Logger.getLogger(LocationMapper.class);
		log.info("Mapping " + data.length + " keys by location.");

		PersistentIntIntArrayMap simple = PersistentIntIntArrayMap.getMap(
				mapName, cluster);

		HashMap<ClientNode, TIntArrayList> map = simple.hashKeys(data);
		log.info("Finished mapping " + data.length + " keys by location.");
		return map;
	}

}
