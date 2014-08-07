package edu.jlime.collections.intintarray.client;

import java.util.Timer;
import java.util.TimerTask;

import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.jd.JobCluster;
import gnu.trove.map.hash.TIntObjectHashMap;

public class BundlerClient extends PersistentIntIntArrayMap {

	private int bundle;

	public BundlerClient(StoreConfig config, int bundleSize, JobCluster cluster)
			throws Exception {
		super(config, cluster);
		this.bundle = bundleSize;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				sendQueue();
			}
		}, 0, 1000);
	}

	Timer timer = new Timer("Persistent Int Int Array Map Bundler", true);

	TIntObjectHashMap<int[]> queue = new TIntObjectHashMap<>();

	@Override
	public void set(int k, int[] data) throws Exception {
		synchronized (queue) {
			queue.put(k, data);
			if (queue.size() > bundle)
				sendQueue();
		}

	}

	private void sendQueue() {
		synchronized (queue) {
			try {
				if (!queue.isEmpty()) {
					super.set(queue);
					queue.clear();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}
