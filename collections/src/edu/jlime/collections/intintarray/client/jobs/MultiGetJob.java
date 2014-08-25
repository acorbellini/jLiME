package edu.jlime.collections.intintarray.client.jobs;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.map.hash.TIntObjectHashMap;

public class MultiGetJob implements Job<byte[]> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public MultiGetJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	@Override
	public byte[] call(JobContext ctx, ClientNode peer) throws Exception {
		Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		ByteBuffer writer = new ByteBuffer();
		Store store = (Store) ctx.get(storeName);
		for (int u : kList) {
			writer.putInt(u);
			byte[] valAsBytes = store.load(u);
			if (valAsBytes != null) {
				int[] obtained = DataTypeUtils.byteArrayToIntArray(valAsBytes);
				Arrays.sort(obtained);
				writer.putByteArray(DataTypeUtils.intArrayToByteArray(obtained));
			} else
				writer.putByteArray(new byte[] {});
			// res.put(u, new int[] {});
		}
		log.info("Returning result for MultiGetJob with " + writer.size()
				+ " adyacency lists.");
		return writer.build();
	}

	public static TIntObjectHashMap<int[]> fromBytes(byte[] bytes) {
		TIntObjectHashMap<int[]> ret = new TIntObjectHashMap<>();
		Buffer reader = new ByteBuffer(bytes);
		while (reader.hasRemaining()) {
			int user = reader.getInt();
			byte[] array = reader.getByteArray();
			ret.put(user, DataTypeUtils.byteArrayToIntArray(array));
		}
		return ret;
	}
}