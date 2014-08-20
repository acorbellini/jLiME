package edu.jlime.collections.adjacencygraph.count;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.GraphMR;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

public class DistHashCountMR extends GraphMR<TIntIntHashMap, byte[]> {

	private static final long serialVersionUID = 3725467268405683770L;

	private String hash;

	private Semaphore lock;

	private GetType type;

	TIntIntHashMap result = new TIntIntHashMap();

	ReentrantLock resultLock = new ReentrantLock();

	public DistHashCountMR(int[] data, String mapName, Mapper mapper,
			GetType type) {
		super(data, mapName, mapper);
		super.setDontCacheSubResults(true);
		this.hash = "CountHash - " + UUID.randomUUID();
		this.type = type;
	}

	@Override
	public void processSubResult(byte[] subres) {
		TIntIntHashMap subHash = DistHashCountJob.fromBytes(subres);
		resultLock.lock();
		for (int k : subHash.keys()) {
			int v = subHash.get(k);
			result.adjustOrPutValue(k, v, v);
		}
		subHash.clear();
		resultLock.unlock();
	}

	@Override
	public Map<Job<byte[]>, ClientNode> map(int[] data, JobContext cluster)
			throws Exception {

		int[] inverted = Arrays.copyOf(data, data.length);

		if (type.equals(GetType.FOLLOWERS))
			for (int i = 0; i < data.length; i++) {
				inverted[i] = -1 * data[i];
			}

		Map<Job<byte[]>, ClientNode> res = new HashMap<>();
		Map<ClientNode, TIntArrayList> map = getMapper().map(inverted, cluster);
		for (Entry<ClientNode, TIntArrayList> e : map.entrySet()) {
			res.put(new DistHashCountJob(e.getValue().toArray(), hash,
					getMapName()), e.getKey());
		}
		lock = new Semaphore(-map.size() + 1);
		return res;
	}

	@Override
	public TIntIntHashMap red(ArrayList<byte[]> subres) {
		try {
			lock.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result;
	}

}