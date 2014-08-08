package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.adjacencygraph.query.StreamForkJoin.StreamJobFactory;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.StreamJob;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

public class RemoteCountQuery extends CompositeQuery<int[], TIntIntHashMap>
		implements CountQuery {

	private static final int CACHE_THREASHOLD = 5000000;

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
		final TIntIntHashMap res = new TIntIntHashMap();
		StreamForkJoin sfj = new StreamForkJoin() {
			@Override
			protected void sendOutput(RemoteOutputStream os, JobNode p) {
				log.info("Sending followers/followees to count to " + p);
				try {
					DataOutputStream dos = RemoteOutputStream.getBDOS(os);
					for (int d : map.get(p).toArray())
						dos.writeInt(d);

					log.info("Closing os.");
					dos.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			}

			@Override
			protected void receiveInput(RemoteInputStream is, JobNode p) {
				DataInputStream dis = RemoteInputStream.getBDIS(is);
				TIntIntHashMap cached = new TIntIntHashMap();
				try {
					while (true) {
						int k = dis.readInt();
						int v = dis.readInt();
						cached.adjustOrPutValue(k, v, v);
						if (cached.size() > CACHE_THREASHOLD) {
							flushCache(res, cached);
						}
					}
				} catch (EOFException e) {
					log.info("Finished reading.");
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (!cached.isEmpty())
					flushCache(res, cached);

			}

			private void flushCache(final TIntIntHashMap res,
					TIntIntHashMap cached) {
				synchronized (res) {
					for (int cachedk : cached.keys()) {
						int cachedv = cached.get(cachedk);
						res.adjustOrPutValue(cachedk, cachedv, cachedv);
					}
				}
				cached.clear();
			}

		};
		sfj.execute(new ArrayList<>(map.keySet()), new StreamJobFactory() {
			@Override
			public StreamJob getStreamJob() {
				return new CountStreamJob(getMapName());
			}
		});
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
