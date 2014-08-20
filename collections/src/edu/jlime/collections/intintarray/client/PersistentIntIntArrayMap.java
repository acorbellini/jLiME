package edu.jlime.collections.intintarray.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.client.jobs.GetJob;
import edu.jlime.collections.intintarray.client.jobs.GetSetOfUsersJob;
import edu.jlime.collections.intintarray.client.jobs.MultiGetJob;
import edu.jlime.collections.intintarray.client.jobs.MultiSetJob;
import edu.jlime.collections.intintarray.client.jobs.PersistentIntIntArrayInitJob;
import edu.jlime.collections.intintarray.client.jobs.SetJob;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.Buffer;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.hash.TIntHashSet;

public class PersistentIntIntArrayMap {

	private static final int READ_BUFFER_SIZE = 128 * 1024;

	Logger log = Logger.getLogger(PersistentIntIntArrayMap.class);

	private ClientCluster cluster;

	private String store;

	public PersistentIntIntArrayMap(StoreConfig config, ClientCluster cluster)
			throws Exception {
		this.cluster = cluster;
		this.store = getName(config.getStoreName());
		cluster.broadcast(new PersistentIntIntArrayInitJob(store, config));
	}

	public void set(int k, int[] data) throws Exception {
		hashKey(k).execAsync(new SetJob(k, data, store));
	}

	public ClientNode hashKey(int k) {
		return getNode(k, cluster.getExecutors());
	}

	private ClientNode getNode(int k, ArrayList<ClientNode> ordered) {
		int index = Math.abs(k % ordered.size());
		return ordered.get(index);
	}

	public int[] get(int k) throws Exception {
		int[] arr = hashKey(k).exec(new GetJob(k, store));
		TIntObjectHashMap<int[]> map = new TIntObjectHashMap<>();
		map.put(k, arr);
		return arr;
	}

	public HashMap<ClientNode, TIntArrayList> hashKeys(int[] userList) {
		ArrayList<ClientNode> ordered = cluster.getExecutors();
		HashMap<ClientNode, TIntArrayList> ret = new HashMap<>();
		for (int u : userList) {
			ClientNode addr = getNode(u, ordered);
			TIntArrayList l = ret.get(addr);
			if (l == null) {
				l = new TIntArrayList();
				ret.put(addr, l);
			}
			l.add(u);
		}
		return ret;
	}

	public TIntObjectHashMap<int[]> get(int[] array) throws Exception {
		ArrayList<Future<byte[]>> list = new ArrayList<>();
		TIntObjectHashMap<int[]> res = new TIntObjectHashMap<int[]>();
		HashMap<ClientNode, TIntArrayList> byServer = hashKeys(array);
		if (log.isDebugEnabled())
			log.debug("Obtaining Futures of executing MultiGetJob");
		for (Entry<ClientNode, TIntArrayList> map : byServer.entrySet()) {
			list.add(map.getKey().execAsyncWithFuture(
					new MultiGetJob(map.getValue().toArray(), store)));
		}

		while (!list.isEmpty()) {
			Future<byte[]> future = null;
			for (Future<byte[]> f : list) {
				if (f.isDone())
					future = f;
			}
			if (future == null) {
				if (log.isDebugEnabled())
					log.debug("No future finished, waiting 100 ms.");
				Thread.sleep(100);
			} else {
				if (log.isDebugEnabled())
					log.debug("Adding results from future.");
				// res.putAll(future.get());
				TIntObjectHashMap<int[]> table = MultiGetJob.fromBytes(future
						.get());
				TIntObjectIterator<int[]> it = table.iterator();
				while (it.hasNext()) {
					it.advance();
					res.put(it.key(), it.value());
					it.remove();
				}
				list.remove(future);
			}
		}
		return res;
	}

	public TIntHashSet getSetOfUsers(int[] array) throws Exception {

		HashMap<ClientNode, TIntArrayList> byServer = hashKeys(array);
		log.info("Starting getSetOfUsers");

		ForkJoinTask<int[]> mgr = new ForkJoinTask<>();
		for (Entry<ClientNode, TIntArrayList> map : byServer.entrySet()) {
			ClientNode p = map.getKey();
			GetSetOfUsersJob j = new GetSetOfUsersJob(map.getValue().toArray(),
					store);
			mgr.putJob(j, p);
		}
		return mgr.execute(new ResultListener<int[], TIntHashSet>() {
			TIntHashSet hashToReturn = null;

			@Override
			public void onSuccess(int[] res) {
				synchronized (this) {
					if (hashToReturn == null)
						hashToReturn = new TIntHashSet(res);
					else
						hashToReturn.addAll(res);
				}
			}

			@Override
			public TIntHashSet onFinished() {
				return hashToReturn;
			}

			@Override
			public void onFailure(Exception res) {
				res.printStackTrace();
			}
		});
	}

	public void set(final TIntObjectHashMap<int[]> orig) throws Exception {
		HashMap<ClientNode, TIntArrayList> byServer = hashKeys(orig.keys());

		for (Entry<ClientNode, TIntArrayList> map : byServer.entrySet()) {
			final TIntObjectHashMap<int[]> toAdd = new TIntObjectHashMap<>();
			map.getValue().forEach(new TIntProcedure() {
				@Override
				public boolean execute(int k) {
					toAdd.put(k, orig.get(k));
					return true;
				}
			});
			map.getKey().exec(new MultiSetJob(toAdd, store));
		}
	}

	public void batchSet(int k, SetProcedure setProcedure) throws Exception {
		while (!setProcedure.finishCondition()) {
			int groupedCount = 0;
			TIntObjectHashMap<int[]> grouped = new TIntObjectHashMap<>();
			while (!setProcedure.finishCondition() && groupedCount <= k) {
				DataEntry entry = setProcedure.getData();
				grouped.put(entry.getKey(), entry.getValue());
			}
			set(grouped);
		}
	}

	public void batchSet(int i, TIntObjectHashMap<int[]> orig) throws Exception {
		final TIntObjectIterator<int[]> it = orig.iterator();
		batchSet(i, new SetProcedure() {

			public DataEntry getData() {
				it.advance();
				return new DataEntry(it.key(), it.value());
			}

			public boolean finishCondition() {
				return !it.hasNext();
			}
		});
	}

	public static PersistentIntIntArrayMap getMap(String map, JobContext c) {
		try {
			return (PersistentIntIntArrayMap) c.get(map);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getName(String storeName) {
		return "DB " + storeName;
	}

	public void close() throws Exception {
		cluster.broadcast(new PersistentIntIntArrayCloseJob(store));
	}

	public TIntIntHashMap countLists(int[] array) throws Exception {
		HashMap<ClientNode, TIntArrayList> byServer = hashKeys(array);
		log.info("Starting CountListsJob");

		ForkJoinTask<TIntIntHashMap> mgr = new ForkJoinTask<>();
		for (Entry<ClientNode, TIntArrayList> map : byServer.entrySet()) {
			ClientNode p = map.getKey();
			CountListsJob j = new CountListsJob(map.getValue().toArray(), store);
			mgr.putJob(j, p);
		}
		return mgr
				.execute(new ResultListener<TIntIntHashMap, TIntIntHashMap>() {
					TIntIntHashMap hashToReturn = null;
					ReentrantLock lock = new ReentrantLock();

					@Override
					public void onSuccess(TIntIntHashMap res) {
						log.info("Received countLists subresult.");
						lock.lock();
						if (hashToReturn == null)
							hashToReturn = res;
						else {
							TIntIntIterator it = res.iterator();
							while (it.hasNext()) {
								it.advance();
								hashToReturn.adjustOrPutValue(it.key(),
										it.value(), it.value());
							}
						}
						lock.unlock();
					}

					@Override
					public TIntIntHashMap onFinished() {
						return hashToReturn;
					}

					@Override
					public void onFailure(Exception res) {
						res.printStackTrace();
					}
				});
	}

	private static class GetAdyacencyListStreamJob extends StreamJob {

		private String name;

		public GetAdyacencyListStreamJob(String name) {
			this.name = name;
		}

		@Override
		public void run(RemoteInputStream input,
				RemoteOutputStream outputStream, JobContext ctx)
				throws Exception {

			// BufferedInputStream input = new BufferedInputStream(inputStream);
			Logger log = Logger.getLogger(MultiGetJob.class);
			TIntArrayList kList = new TIntArrayList();
			try {
				byte[] buffer = new byte[READ_BUFFER_SIZE];
				int read = 0;
				while ((read = input.read(buffer)) != -1)
					for (int i = 0; i < read / 4; i++) {
						int k = DataTypeUtils.byteArrayToInt(buffer, i * 4);
						kList.add(k);
					}
			} catch (Exception e) {
			}
			log.info("Obtaining multiple keys (" + kList.size()
					+ ") from store");
			TIntArrayList set = new TIntArrayList();

			Buffer buffer = new ByteBuffer();
			int max = 256 * 1024;
			Store store = (Store) ctx.get(name);
			TIntIterator it = kList.iterator();
			while (it.hasNext()) {
				int u = it.next();
				byte[] valAsBytes = store.load(u);
				if (valAsBytes != null) {
					set.addAll(DataTypeUtils.byteArrayToIntArray(valAsBytes));
				}
			}

			int[] array = set.toArray();
			byte[] intArrayToByteArray = DataTypeUtils
					.intArrayToByteArray(array);
			set.clear();
			log.info("Sending multiple keys " + array.length + " from store");
			outputStream.write(intArrayToByteArray);
			log.info("Finished obtaining multiple keys (" + kList.size()
					+ ") from store");
			outputStream.close();
		}
	}

	public void list() throws Exception {
		ForkJoinTask<Boolean> mgr = new ForkJoinTask<>();
		for (ClientNode j : cluster.getExecutors()) {
			ClientNode p = j;
			ListJob list = new ListJob(store);
			mgr.putJob(list, p);
		}
		mgr.execute(new ResultListener<Boolean, Void>() {

			@Override
			public void onSuccess(Boolean result) {
				// TODO Auto-generated method stub

			}

			@Override
			public Void onFinished() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void onFailure(Exception res) {
				// TODO Auto-generated method stub

			}
		});
	}
}
