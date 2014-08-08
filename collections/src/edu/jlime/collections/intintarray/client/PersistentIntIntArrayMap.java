package edu.jlime.collections.intintarray.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.query.StreamForkJoin;
import edu.jlime.collections.adjacencygraph.query.StreamForkJoin.StreamJobFactory;
import edu.jlime.collections.intintarray.client.jobs.GetJob;
import edu.jlime.collections.intintarray.client.jobs.MultiGetJob;
import edu.jlime.collections.intintarray.client.jobs.MultiSetJob;
import edu.jlime.collections.intintarray.client.jobs.PersistentIntIntArrayInitJob;
import edu.jlime.collections.intintarray.client.jobs.SetJob;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.hash.TIntHashSet;

public class PersistentIntIntArrayMap {

	private static final int CACHED_THRESHOLD = 5000;

	Logger log = Logger.getLogger(PersistentIntIntArrayMap.class);

	private JobCluster cluster;

	private String store;

	public PersistentIntIntArrayMap(StoreConfig config, JobCluster cluster)
			throws Exception {
		this.cluster = cluster;
		this.store = getName(config.getStoreName());
		cluster.broadcast(new PersistentIntIntArrayInitJob(store, config));
	}

	public void set(int k, int[] data) throws Exception {
		// TIntArrayList list = new TIntArrayList();
		// list.add(k);
		// if (cache.get(list) == null) {
		// TIntObjectHashMap<int[]> toAdd = new TIntObjectHashMap<>();
		// toAdd.put(k, data);
		// cache.put(list, toAdd);
		// }
		hashKey(k).exec(new SetJob(k, data, store));
	}

	public JobNode hashKey(int k) {
		ArrayList<JobNode> ordered = cluster.getExecutors();
		Collections.sort(ordered, new Comparator<JobNode>() {
			@Override
			public int compare(JobNode o1, JobNode o2) {
				Integer i1 = Integer.valueOf(o1.getName().replaceAll(
						"GridCluster", ""));
				Integer i2 = Integer.valueOf(o2.getName().replaceAll(
						"GridCluster", ""));
				return i1.compareTo(i2);
			}
		});
		return ordered.get(Math.abs(k % ordered.size()));
	}

	public int[] get(int k) throws Exception {
		int[] arr = hashKey(k).exec(new GetJob(k, store));
		TIntObjectHashMap<int[]> map = new TIntObjectHashMap<>();
		map.put(k, arr);
		return arr;
	}

	public HashMap<JobNode, TIntArrayList> getAffinityNode(int[] userList) {
		HashMap<JobNode, TIntArrayList> ret = new HashMap<JobNode, TIntArrayList>();
		for (int u : userList) {
			JobNode addr = hashKey(u);
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
		HashMap<JobNode, TIntArrayList> byServer = getAffinityNode(array);
		if (log.isDebugEnabled())
			log.debug("Obtaining Futures of executing MultiGetJob");
		for (Entry<JobNode, TIntArrayList> map : byServer.entrySet()) {
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
		final TIntHashSet res = new TIntHashSet();
		final HashMap<JobNode, TIntArrayList> byServer = getAffinityNode(array);

		StreamForkJoin sfj = new StreamForkJoin() {
			@Override
			protected void sendOutput(RemoteOutputStream os, JobNode p) {
				DataOutputStream dos = RemoteOutputStream.getBDOS(os);
				TIntArrayList list = byServer.get(p);
				try {
					for (int i : list.toArray())
						dos.writeInt(i);
					dos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			protected void receiveInput(RemoteInputStream is, JobNode p) {
				DataInputStream dis = RemoteInputStream.getBDIS(is);
				TIntHashSet cached = new TIntHashSet();
				try {
					while (true) {
						int k = dis.readInt();
						cached.add(k);
						if (cached.size() > CACHED_THRESHOLD) {
							flushCache(res, cached);
						}
					}
				} catch (EOFException e) {
					if (log.isDebugEnabled())
						log.debug("Finished reading.");
				} catch (Exception e) {
					log.error("", e);
				}
				if (!cached.isEmpty())
					flushCache(res, cached);
			}

			private void flushCache(final TIntHashSet res, TIntHashSet cached) {
				synchronized (res) {
					for (int k : cached.toArray()) {
						res.add(k);
					}
				}
				cached.clear();
			}
		};
		sfj.execute(new ArrayList<>(byServer.keySet()), new StreamJobFactory() {
			@Override
			public StreamJob getStreamJob() {
				return new GetAdyacencyListStreamJob(store);
			}
		});

		// if (log.isDebugEnabled())
		// log.debug("Obtaining Futures of executing GetSetOfUsersJob for getSetOfUsers");
		//
		// for (Entry<Peer, TIntArrayList> map : byServer.entrySet()) {
		// list.add(map.getKey().execAsyncWithFuture(
		// new GetSetOfUsersJob(map.getValue().toArray(), store)));
		// }
		// while (!list.isEmpty()) {
		// Future<int[]> future = null;
		// for (Future<int[]> f : list)
		// if (f.isDone())
		// future = f;
		//
		// if (future == null) {
		// if (log.isDebugEnabled())
		// log.debug("No future finished, waiting 100 ms.");
		// Thread.sleep(100);
		// } else {
		// if (log.isDebugEnabled())
		// log.debug("Adding results from future.");
		// res.addAll(future.get());
		// list.remove(future);
		// }
		// }
		if (log.isDebugEnabled())
			log.debug("Finished getSetOfUsers");
		return res;
	}

	public void set(final TIntObjectHashMap<int[]> orig) throws Exception {
		HashMap<JobNode, TIntArrayList> byServer = getAffinityNode(orig.keys());

		for (Entry<JobNode, TIntArrayList> map : byServer.entrySet()) {
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
		// final TIntIntHashMap hashToReturn = new TIntIntHashMap();
		// HashMap<Peer, TIntArrayList> byServer = getAffinityNode(array);
		// log.info("Obtaining Futures of executing CountListsJob");
		//
		// ForkJoinTask<byte[]> mgr = new ForkJoinTask<>();
		// for (Entry<Peer, TIntArrayList> map : byServer.entrySet()) {
		// Peer p = map.getKey();
		// CountListsJob j = new CountListsJob(map.getValue().toArray(), store);
		// mgr.putJob(j, p);
		// }
		// mgr.execute(new ResultListener<byte[]>() {
		//
		// @Override
		// public void onSuccess(byte[] res) {
		// TIntIntHashMap table = c.fromBytes(res);
		// synchronized (hashToReturn) {
		// if (hashToReturn.isEmpty())
		// hashToReturn.putAll(table);
		// else {
		// for (int k : table.keys()) {
		// int v = table.get(k);
		// hashToReturn.adjustOrPutValue(k, v, v);
		// }
		// }
		// }
		// }
		//
		// @Override
		// public void onFinished() {
		// }
		//
		// @Override
		// public void onFailure(Exception res) {
		// }
		// });
		// return hashToReturn;

		final TIntIntHashMap hashToReturn = new TIntIntHashMap();
		final HashMap<JobNode, TIntArrayList> byServer = getAffinityNode(array);
		StreamForkJoin sfj = new StreamForkJoin() {
			@Override
			protected void sendOutput(RemoteOutputStream os, JobNode p) {
				DataOutputStream dos = RemoteOutputStream.getBDOS(os);
				TIntArrayList list = byServer.get(p);
				try {

					log.info("Sending key stream to get from local store.");

					for (int i : list.toArray())
						dos.writeInt(i);

					log.info("Finished sending key stream to get from local store.");

					dos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			protected void receiveInput(RemoteInputStream is, JobNode p) {
				DataInputStream dis = RemoteInputStream.getBDIS(is);
				TIntIntHashMap cached = new TIntIntHashMap();
				try {
					while (true) {
						int k = dis.readInt();
						cached.adjustOrPutValue(k, 1, 1);
						if (cached.size() > CACHED_THRESHOLD) {
							flushCache(hashToReturn, cached);
						}
					}
				} catch (EOFException e) {
					// if (log.isDebugEnabled())
					log.info("Finished obtaining remote store keys.");
				} catch (Exception e) {
					log.error("", e);
				} finally {
					try {
						dis.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (!cached.isEmpty())
					flushCache(hashToReturn, cached);
			}

			private void flushCache(final TIntIntHashMap hashToReturn,
					TIntIntHashMap cached) {
				synchronized (hashToReturn) {
					for (int cachedk : cached.keys()) {
						int cachedv = cached.get(cachedk);
						hashToReturn
								.adjustOrPutValue(cachedk, cachedv, cachedv);
					}
				}
				cached.clear();
			}
		};
		sfj.execute(new ArrayList<>(byServer.keySet()), new StreamJobFactory() {
			@Override
			public StreamJob getStreamJob() {
				return new GetAdyacencyListStreamJob(store);
			}
		});
		// if (log.isDebugEnabled())
		log.info("Returning count hash with " + hashToReturn.size());
		return hashToReturn;
	}

	private static class GetAdyacencyListStreamJob extends StreamJob {

		private String name;

		public GetAdyacencyListStreamJob(String name) {
			this.name = name;
		}

		@Override
		public void run(RemoteInputStream inputStream,
				RemoteOutputStream outputStream, JobContext ctx)
				throws Exception {

			DataInputStream dis = RemoteInputStream.getBDIS(inputStream);
			Logger log = Logger.getLogger(MultiGetJob.class);
			TIntArrayList kList = new TIntArrayList();
			try {
				while (true) {
					kList.add(dis.readInt());
				}
			} catch (Exception e) {
			}
			DataOutputStream dos = RemoteOutputStream.getBDOS(outputStream);
			// if (log.isDebugEnabled())
			log.info("Obtaining multiple keys (" + kList.size()
					+ ") from store");
			Store store = (Store) ctx.get(name);

			for (int u : kList.toArray()) {
				byte[] valAsBytes = store.load(u);
				if (valAsBytes != null) {
					int[] obtained = DataTypeUtils
							.byteArrayToIntArray(valAsBytes);
					for (int i : obtained) {
						dos.writeInt(i);
					}

				}
			}
			log.info("Finished obtaining multiple keys (" + kList.size()
					+ ") from store");
			dos.close();
		}

	}
}
