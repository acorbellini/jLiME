package edu.jlime.collections.intintarray.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;

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
import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.hash.TIntHashSet;

public class PersistentIntIntArrayMap {

	private static final int READ_BUFFER_SIZE = 128 * 1024;

	private static final int CACHED_THRESHOLD = 10000;

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
		return getNode(k, ordered);
	}

	private JobNode getNode(int k, ArrayList<JobNode> ordered) {
		return ordered.get(Math.abs(k % ordered.size()));
	}

	public int[] get(int k) throws Exception {
		int[] arr = hashKey(k).exec(new GetJob(k, store));
		TIntObjectHashMap<int[]> map = new TIntObjectHashMap<>();
		map.put(k, arr);
		return arr;
	}

	public HashMap<JobNode, TIntArrayList> hashKeys(int[] userList) {
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
		HashMap<JobNode, TIntArrayList> ret = new HashMap<JobNode, TIntArrayList>();
		for (int u : userList) {
			JobNode addr = getNode(u, ordered);
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
		HashMap<JobNode, TIntArrayList> byServer = hashKeys(array);
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

		HashMap<JobNode, TIntArrayList> byServer = hashKeys(array);
		log.info("Starting getSetOfUsers");

		ForkJoinTask<TIntHashSet> mgr = new ForkJoinTask<>();
		for (Entry<JobNode, TIntArrayList> map : byServer.entrySet()) {
			JobNode p = map.getKey();
			GetSetOfUsersJob j = new GetSetOfUsersJob(map.getValue().toArray(),
					store);
			mgr.putJob(j, p);
		}
		return mgr.execute(new ResultListener<TIntHashSet, TIntHashSet>() {
			TIntHashSet hashToReturn = new TIntHashSet();

			@Override
			public void onSuccess(TIntHashSet res) {
				synchronized (hashToReturn) {
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
		// final TIntHashSet res = new TIntHashSet();
		// final HashMap<JobNode, TIntArrayList> byServer = hashKeys(array);
		//
		// StreamForkJoin sfj = new StreamForkJoin() {
		// @Override
		// protected void send(RemoteOutputStream dos, JobNode p) {
		// // BufferedOutputStream dos = new BufferedOutputStream(os);
		// TIntArrayList list = byServer.get(p);
		// try {
		// dos.write(IntUtils.intArrayToByteArray(list.toArray()));
		// dos.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		//
		// @Override
		// protected void receive(RemoteInputStream input, JobNode p) {
		// // BufferedInputStream input = new BufferedInputStream(is);
		// TIntHashSet cached = new TIntHashSet();
		// try {
		//
		// byte[] buffer = new byte[READ_BUFFER_SIZE];
		// int read = 0;
		// while ((read = input.read(buffer)) != -1)
		// for (int i = 0; i < read / 4; i++) {
		// int k = IntUtils.byteArrayToInt(buffer, i * 4);
		// cached.add(k);
		// if (cached.size() > CACHED_THRESHOLD) {
		// flushCache(res, cached);
		// }
		// }
		// } catch (EOFException e) {
		// if (log.isDebugEnabled())
		// log.debug("Finished reading.");
		// } catch (Exception e) {
		// log.error("", e);
		// } finally {
		// try {
		// input.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		// if (!cached.isEmpty())
		// flushCache(res, cached);
		// }
		//
		// private void flushCache(final TIntHashSet res, TIntHashSet cached) {
		// synchronized (res) {
		// for (int k : cached.toArray()) {
		// res.add(k);
		// }
		// }
		// cached.clear();
		// }
		// };
		// sfj.execute(new ArrayList<>(byServer.keySet()), new
		// StreamJobFactory() {
		// @Override
		// public StreamJob getStreamJob() {
		// return new GetAdyacencyListStreamJob(store);
		// }
		// });

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
		// if (log.isDebugEnabled())
		// log.info("Finished getSetOfUsers");
		// return res;
	}

	public void set(final TIntObjectHashMap<int[]> orig) throws Exception {
		HashMap<JobNode, TIntArrayList> byServer = hashKeys(orig.keys());

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
		HashMap<JobNode, TIntArrayList> byServer = hashKeys(array);
		log.info("Starting CountListsJob");

		ForkJoinTask<TIntIntHashMap> mgr = new ForkJoinTask<>();
		for (Entry<JobNode, TIntArrayList> map : byServer.entrySet()) {
			JobNode p = map.getKey();
			CountListsJob j = new CountListsJob(map.getValue().toArray(), store);
			mgr.putJob(j, p);
		}
		return mgr
				.execute(new ResultListener<TIntIntHashMap, TIntIntHashMap>() {
					List<TIntIntHashMap> subres = new ArrayList<>();

					@Override
					public void onSuccess(TIntIntHashMap res) {
						log.info("Received countLists subresult.");
						synchronized (subres) {
							subres.add(res);
						}
					}

					@Override
					public TIntIntHashMap onFinished() {
						if (subres.size() == 1)
							return subres.get(0);
						TIntIntHashMap hashToReturn = new TIntIntHashMap();
						for (TIntIntHashMap table : subres) {
							if (hashToReturn.isEmpty())
								hashToReturn.putAll(table);
							else {
								for (int k : table.keys()) {
									int v = table.get(k);
									hashToReturn.adjustOrPutValue(k, v, v);
								}
							}
						}
						log.info("Finished countLists");
						return hashToReturn;
					}

					@Override
					public void onFailure(Exception res) {
						res.printStackTrace();
					}
				});
		// final TIntIntHashMap hashToReturn = new TIntIntHashMap();
		// final HashMap<JobNode, TIntArrayList> byServer = hashKeys(array);
		// StreamForkJoin sfj = new StreamForkJoin() {
		// @Override
		// protected void send(RemoteOutputStream os, JobNode p) {
		// BufferedOutputStream dos = new BufferedOutputStream(os);
		// TIntArrayList list = byServer.get(p);
		// try {
		//
		// log.info("Sending key stream to get from local store.");
		// dos.write(IntUtils.intArrayToByteArray(list.toArray()));
		// log.info("Finished sending key stream to get from local store.");
		//
		// dos.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		//
		// @Override
		// protected void receive(RemoteInputStream input, JobNode p) {
		// // BufferedInputStream input = new BufferedInputStream(is);
		// TIntIntHashMap cached = new TIntIntHashMap();
		// try {
		// byte[] buffer = new byte[READ_BUFFER_SIZE];
		// int read = 0;
		// while ((read = input.read(buffer)) != -1)
		// for (int i = 0; i < read / 4; i++) {
		// int k = IntUtils.byteArrayToInt(buffer, i * 4);
		// cached.adjustOrPutValue(k, 1, 1);
		// if (cached.size() > CACHED_THRESHOLD) {
		// flushCache(hashToReturn, cached);
		// }
		// }
		// } catch (EOFException e) {
		// // if (log.isDebugEnabled())
		//
		// } catch (Exception e) {
		// log.error("", e);
		// } finally {
		// try {
		// input.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		// log.info("Finished obtaining remote store keys.");
		// if (!cached.isEmpty())
		// flushCache(hashToReturn, cached);
		// }
		//
		// private void flushCache(final TIntIntHashMap hashToReturn,
		// TIntIntHashMap cached) {
		// synchronized (hashToReturn) {
		// for (int cachedk : cached.keys()) {
		// int cachedv = cached.get(cachedk);
		// hashToReturn
		// .adjustOrPutValue(cachedk, cachedv, cachedv);
		// }
		// }
		// cached.clear();
		// }
		// };
		// sfj.execute(new ArrayList<>(byServer.keySet()), new
		// StreamJobFactory() {
		// @Override
		// public StreamJob getStreamJob() {
		// return new GetAdyacencyListStreamJob(store);
		// }
		// });
		// // if (log.isDebugEnabled())
		// log.info("Returning count hash with " + hashToReturn.size());
		// return hashToReturn;
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

			ByteBuffer buffer = new ByteBuffer();
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
}
