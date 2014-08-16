package edu.jlime.collections.adjacencygraph.count;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;
import gnu.trove.map.hash.TIntIntHashMap;

public class DistHashCountJob implements Job<byte[]> {

	private static final long serialVersionUID = 5818660842282020979L;

	private int[] data;

	String map;

	public DistHashCountJob(int[] data, String hash, String map) {
		this.data = data;
		this.map = map;
	}

	@Override
	public byte[] call(JobContext ctx, JobNode peer) throws Exception {

		Logger log = Logger.getLogger(DistHashCountJob.class);

		log.info("Counting " + data.length + " users.");

		PersistentIntIntArrayMap dkvs = PersistentIntIntArrayMap.getMap(map,
				ctx);

		TIntIntHashMap adyacents = null;
		try {
			log.info("Calling DKVS get.");
			adyacents = dkvs.countLists(data);
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		log.info("Finished calling DKVS get, obtained " + adyacents.size());
		if (adyacents.size() == 0) {
			log.info("No data in adyacency list.");
			try {
				return new byte[] {};
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// DistIntIntHashtable resHash = DistIntIntHashtable.get(hash, ctx);
		// log.info("Building Hashtable with temporal results");
		// for (int k : adyacents.keys())
		// resHash.batchPutOrAdd(k, adyacents.get(k));
		//
		// adyacents.clear();
		// resHash.waitForTermination();
		ByteBuffer writer = new ByteBuffer();
		for (int k : adyacents.keys()) {
			writer.putInt(k);
			writer.putInt(adyacents.get(k));
		}
		return writer.build();
	}

	public static TIntIntHashMap fromBytes(byte[] bytes) {
		TIntIntHashMap hash = new TIntIntHashMap();
		Buffer reader = new ByteBuffer(bytes);
		while (reader.hasRemaining()) {
			hash.put(reader.getInt(), reader.getInt());
		}
		return hash;
	}
}
