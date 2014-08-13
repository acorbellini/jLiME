package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.adjacencygraph.query.StreamForkJoin.StreamJobFactory;
import edu.jlime.collections.intintarray.client.CountListsJob;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.jd.task.ForkJoinTask;
import edu.jlime.jd.task.ResultListener;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class RemoteCountQuery extends CompositeQuery<int[], TIntIntHashMap>
		implements CountQuery {

	private static final int READ_BUFFER_SIZE = 32 * 1024;

	private static final int CACHE_THRESHOLD = 4 * 8 * 1000;

	private static final long serialVersionUID = 5030949972656440876L;

	private GetType type;

	private RemoteListQuery toremove;

	public RemoteCountQuery(RemoteQuery<int[]> query, GetType type) {
		super(query);
		super.setCacheQuery(false);
		this.type = type;
	}

	@Override
	public TIntIntHashMap doExec(JobContext c) throws Exception {
		final Logger log = Logger.getLogger(RemoteCountQuery.class);
		int[] data = getQuery().exec(c);
		int[] inverted = Arrays.copyOf(data, data.length);
		if (type.equals(GetType.FOLLOWERS))
			for (int i = 0; i < data.length; i++)
				inverted[i] = -1 * data[i];

		final Map<JobNode, TIntArrayList> map = getMapper().map(inverted, c);

		ForkJoinTask<TIntArrayList> fjt = new ForkJoinTask<TIntArrayList>();
		for (Entry<JobNode, TIntArrayList> e : map.entrySet()) {
			JobNode p = e.getKey();
			CountJob j = new CountJob(map.getValue().toArray(), store);
			fjt.putJob(j, p);
		}

		final TIntIntHashMap res = fjt
				.execute(new ResultListener<TIntArrayList, TIntIntHashMap>() {

					@Override
					public void onSuccess(TIntArrayList result) {
						// TODO Auto-generated method stub

					}

					@Override
					public TIntIntHashMap onFinished() {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public void onFailure(Exception res) {
						// TODO Auto-generated method stub

					}
				});
		// StreamForkJoin sfj = new StreamForkJoin() {
		// @Override
		// protected void send(RemoteOutputStream os, JobNode p) {
		// log.info("Sending followers/followees to count to " + p);
		// try {
		// // BufferedOutputStream dos = new BufferedOutputStream(os);
		// os.write(DataTypeUtils.intArrayToByteArray(map.get(p)
		// .toArray()));
		// log.info("RemoteCountQuery: Finished sending followers/followees to count to "
		// + p);
		// os.close();
		// } catch (IOException e1) {
		// e1.printStackTrace();
		// }
		//
		// }
		//
		// @Override
		// protected void receive(RemoteInputStream input, JobNode p) {
		// // BufferedInputStream input = new BufferedInputStream(is);
		// log.info("Receiving Count Stream Job from " + p);
		// TIntIntHashMap cached = new TIntIntHashMap();
		// try {
		// byte[] buffer = new byte[READ_BUFFER_SIZE];
		// int read = 0;
		// while ((read = input.read(buffer)) != -1)
		// for (int i = 0; i < read / 4; i += 2) {
		// int k = DataTypeUtils.byteArrayToInt(buffer, i * 4);
		// int v = DataTypeUtils.byteArrayToInt(buffer,
		// i * 4 + 4);
		// cached.adjustOrPutValue(k, v, v);
		// if (cached.size() > CACHE_THRESHOLD) {
		// System.out.println("Flushing cache.");
		// flushCache(res, cached);
		// }
		// }
		// } catch (EOFException e) {
		//
		// } catch (Exception e) {
		// e.printStackTrace();
		// } finally {
		// try {
		// input.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		// log.info("Finished reading from " + p);
		// if (!cached.isEmpty())
		// flushCache(res, cached);
		//
		// }
		//
		// private void flushCache(final TIntIntHashMap res,
		// TIntIntHashMap cached) {
		// synchronized (res) {
		// for (int cachedk : cached.keys()) {
		// int cachedv = cached.get(cachedk);
		// res.adjustOrPutValue(cachedk, cachedv, cachedv);
		// }
		// }
		// cached.clear();
		// }
		//
		// };
		// sfj.execute(new ArrayList<>(map.keySet()), new StreamJobFactory() {
		// @Override
		// public StreamJob getStreamJob() {
		// return new CountStreamJob(getMapName());
		// }
		// });
		if (toremove != null) {
			for (int u : toremove.exec(c)) {
				res.remove(u);
			}
		}
		return res;
	}

	@Override
	public TopQuery top(int top) {
		return new TopQuery(this, top);
	}

	@Override
	public TopQuery top(int top, boolean delete) {
		return new TopQuery(this, top, delete);
	}

	@Override
	public CountQuery remove(RemoteListQuery followees) throws Exception {
		toremove = followees;
		return this;
	}

	@Override
	public ListQuery getToremove() {
		return toremove;
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof RemoteCountQuery))
			return false;

		RemoteCountQuery other = (RemoteCountQuery) obj;

		return this.getQuery().equals(other.getQuery())
				&& getToremove().equals(other.getToremove());
	}

	@Override
	public int hashCode() {
		return getQuery().hashCode();
	}
}
