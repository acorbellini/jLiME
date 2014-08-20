package edu.jlime.collections.intintarray.client.jobs;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.Job;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.RingQueue;
import gnu.trove.set.hash.TIntHashSet;

public class GetSetOfUsersJob implements Job<int[]> {

	private static final long serialVersionUID = 3437379208216701568L;

	private int[] kList;

	private String storeName;

	public GetSetOfUsersJob(int[] k, String name) {
		this.kList = k;
		this.storeName = name;
	}

	@Override
	public int[] call(JobContext ctx, ClientNode peer) throws Exception {
		final Logger log = Logger.getLogger(MultiGetJob.class);
		log.info("Obtaining multiple keys (" + kList.length + ") from store");
		Store store = (Store) ctx.get(storeName);
		TIntHashSet hash = new TIntHashSet();

		log.info("Loading kList.");
		List<byte[]> loadAll = store.loadAll(kList);
		log.info("Loaded kList.");
		for (byte[] valAsBytes : loadAll) {
			if (valAsBytes != null) {
				int[] byteArrayToIntArray = DataTypeUtils
						.byteArrayToIntArray((byte[]) valAsBytes);
				hash.addAll(byteArrayToIntArray);
			}
		}

		return hash.toArray();
	}
}