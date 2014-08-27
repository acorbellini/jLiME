package edu.jlime.collections.intintarray.client;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.client.jobs.MultiGetJob;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.map.hash.TIntIntHashMap;

public class CountListsJob implements Job<TIntIntHashMap> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public CountListsJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	@Override
	public TIntIntHashMap call(JobContext ctx, ClientNode peer)
			throws Exception {
		final Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");

		Store store = (Store) ctx.get(storeName);

		TIntIntHashMap hash = new TIntIntHashMap();
		log.info("Loading kList.");
		List<byte[]> loadAll = store.loadAll(kList);
		log.info("Loaded kList.");
		for (final byte[] valAsBytes : loadAll) {
			if (valAsBytes != null) {
				int[] intArray = DataTypeUtils
						.byteArrayToIntArray((byte[]) valAsBytes);
				for (int b : intArray)
					hash.adjustOrPutValue(b, 1, 1);
			}

		}
		return hash;
	}

	public static TIntIntHashMap fromBytes(byte[] bytes) {
		TIntIntHashMap res = new TIntIntHashMap();
		Buffer reader = new ByteBuffer(bytes);
		int[] keys = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		int[] values = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		for (int i = 0; i < keys.length; i++) {
			res.put(keys[i], values[i]);
		}
		return res;
	}

}
