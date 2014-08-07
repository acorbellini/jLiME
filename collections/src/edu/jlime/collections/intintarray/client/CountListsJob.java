package edu.jlime.collections.intintarray.client;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.client.jobs.MultiGetJob;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.map.hash.TIntIntHashMap;

public class CountListsJob implements Job<byte[]> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public CountListsJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	@Override
	public byte[] call(JobContext ctx, JobNode peer) throws Exception {
		Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		TIntIntHashMap hash = new TIntIntHashMap();
		Store store = (Store) ctx.get(storeName);
		for (int u : kList) {
			byte[] valAsBytes = store.load(u);
			if (valAsBytes != null) {
				int[] obtained = DataTypeUtils.byteArrayToIntArray(valAsBytes);
				Arrays.sort(obtained);
				for (int i : obtained) {
					hash.adjustOrPutValue(i, 1, 1);
				}
			}
			// res.put(u, new int[] {});
		}
		log.info("Returning result for CountListsJob with " + hash.size()
				+ " users.");
		ByteBuffer writer = new ByteBuffer();
		writer.putByteArray(DataTypeUtils.intArrayToByteArray(hash.keys()));
		writer.putByteArray(DataTypeUtils.intArrayToByteArray(hash.values()));
		return writer.build();
	}

	public static TIntIntHashMap fromBytes(byte[] bytes) {
		TIntIntHashMap res = new TIntIntHashMap();
		ByteBuffer reader = new ByteBuffer(bytes);
		int[] keys = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		int[] values = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		for (int i = 0; i < keys.length; i++) {
			res.put(keys[i], values[i]);
		}
		return res;
	}

}
