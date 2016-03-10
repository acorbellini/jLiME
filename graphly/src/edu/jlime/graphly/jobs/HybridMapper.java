package edu.jlime.graphly.jobs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

public class HybridMapper implements Mapper {

	public static class MapperData implements Serializable {
		public MapperData(Mapper mapper2, float f) {
			this.mapper = mapper2;
			this.div = f;
		}

		Mapper mapper;
		float div;
		Peer[] peers;
		private int peerPos;

		public void update(JobContext ctx, int acc) throws Exception {
			mapper.update(ctx);
			peers = mapper.getPeers();
			peerPos = acc;
		}

		@Override
		public String toString() {
			return "MapperData [mapper=" + mapper + ", div=" + div + ", peers="
					+ peers.length + ", peerPos=" + peerPos + "]";
		}

	}

	MapperData[] mapData;

	public HybridMapper(Mapper[] mappers, float[] divs) {
		mapData = new MapperData[mappers.length];
		for (int i = 0; i < divs.length; i++) {
			mapData[i] = new MapperData(mappers[i], divs[i]);
		}
	}

	private static final int VNODES = 100;

	private Peer[] peers;

	@Override
	public List<Pair<Node, TLongArrayList>> map(int max, long[] data,
			JobContext ctx) throws Exception {

		int acc = 0;
		Map<Node, TLongArrayList> ret = new HashMap<>();
		for (int i = 0; i < mapData.length; i++) {
			MapperData mapperData = mapData[i];
			float range = data.length * mapperData.div;
			int to = (int) (i == mapData.length - 1 ? data.length
					: acc + range);

			List<Pair<Node, TLongArrayList>> subMap = mapperData.mapper.map(max,
					Arrays.copyOfRange(data, acc, to), ctx);

			acc += range;
			for (Pair<Node, TLongArrayList> pair : subMap) {
				TLongArrayList sublist = ret.get(pair.left);
				if (sublist == null) {
					sublist = new TLongArrayList();
					ret.put(pair.left, sublist);
				}
				sublist.addAll(pair.right);
			}
		}

		return GraphlyUtil.divide(ret, max);
	}

	@Override
	public String getName() {
		StringBuilder builder = new StringBuilder();
		for (MapperData mapper : mapData) {
			builder.append((builder.length() == 0 ? "" : ",")
					+ mapper.mapper.getName() + "[" + mapper.div + "]");
		}
		return "hybrid-(" + builder.toString() + ")";
	}

	@Override
	public boolean isDynamic() {
		for (MapperData mapper : mapData) {
			if (mapper.mapper.isDynamic())
				return true;
		}
		return false;
	}

	@Override
	public synchronized void update(JobContext ctx) throws Exception {
		int acc = 0;
		for (MapperData mdata : mapData) {
			mdata.update(ctx, acc);
			acc += mdata.peers.length;
		}

		ArrayList<Peer> peers = new ArrayList<>();
		for (MapperData mapperData : mapData) {
			for (Peer peer : mapperData.peers) {
				peers.add(peer);
			}
		}
		this.peers = peers.toArray(new Peer[] {});

		System.out.println("MapperData: " + Arrays.toString(this.mapData));
	}

	@Override
	public Node getNode(long v, JobContext ctx) {
		return getMapperData(v).mapper.getNode(v, ctx);
	}

	private MapperData getMapperData(long v) {
		float pos = divisionFunction(v);

		float acc = 0;

		for (MapperData mapperData : mapData) {
			acc += mapperData.div;
			if (pos < acc)
				return mapperData;
		}
		return mapData[mapData.length - 1];
	}

	@Override
	public Peer[] getPeers() {
		return peers;
	}

	@Override
	public int hash(long v, JobContext ctx) {
		MapperData mapper = getMapperData(v);
		int hash = mapper.mapper.hash(v, ctx);
		return mapper.peerPos + hash;
	}

	private float divisionFunction(long v) {
		return ((int) (v * 31) % VNODES) / (float) VNODES;
	}
}
