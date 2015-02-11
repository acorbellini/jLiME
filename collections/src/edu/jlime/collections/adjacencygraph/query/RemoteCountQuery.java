package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.collections.adjacencygraph.query.StreamForkJoin.StreamJobFactory;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

public class RemoteCountQuery extends CompositeQuery<int[], TIntIntHashMap>
		implements CountQuery {

	private static final int READ_BUFFER_SIZE = 50 * 1024 * 1024;

	// private static final int CACHED = 1 * 1024 * 1024;

	private static final int HASH_INIT = 1000000;

	private static final long serialVersionUID = 5030949972656440876L;

	private Dir type;

	private RemoteListQuery toremove;

	public RemoteCountQuery(RemoteQuery<int[]> query, Dir type) {
		super(query);
		super.setCacheQuery(false);
		this.type = type;
	}

	@Override
	public TIntIntHashMap doExec(JobContext c) throws Exception {
		final Logger log = Logger.getLogger(RemoteCountQuery.class);
		int[] data = getQuery().exec(c);
		int[] inverted = Arrays.copyOf(data, data.length);
		if (type.equals(Dir.OUT))
			for (int i = 0; i < data.length; i++)
				inverted[i] = -1 * data[i];

		final Map<ClientNode, TIntArrayList> map = getMapper().map(inverted, c);

		final TIntIntHashMap res = new TIntIntHashMap(HASH_INIT, 0.9f);

		StreamForkJoin sfj = new StreamForkJoin() {
			@Override
			protected void send(RemoteOutputStream os, ClientNode p) {
				// log.info("Sending followers/followees to count to " + p);
				try {
					os.write(DataTypeUtils.intArrayToByteArray(map.get(p)
							.toArray()));
					// log.info("RemoteCountQuery: Finished sending followers/followees to count to "
					// + p);
					os.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			}

			@Override
			protected void receive(RemoteInputStream input, ClientNode p) {
				// log.info("Receiving Count Stream Job from " + p);
				// int[] cached = new int[CACHE_THRESHOLD];
				// TByteArrayList cached = new TByteArrayList(CACHED
				// + READ_BUFFER_SIZE);
				try {
					byte[] buffer = new byte[READ_BUFFER_SIZE];
					int read = 0;
					int count = 0;
					while ((read = input.read(buffer, count, buffer.length
							- count)) != -1) {
						count += read;
						if (count == READ_BUFFER_SIZE) {
							flushCache(res, buffer, buffer.length);
							count = 0;
						}
					}
					if (count != 0)
						flushCache(res, buffer, count);
				} catch (EOFException e) {

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				log.info("Finished reading from " + p);

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
		log.info("Finished RemoteCountQuery");
		return res;
	}

	private void flushCache(final TIntIntHashMap res, byte[] cached, int count) {
		synchronized (res) {
			for (int i = 0; i < count / 4; i += 2) {
				int k = DataTypeUtils.byteArrayToInt(cached, i * 4);
				int v = DataTypeUtils.byteArrayToInt(cached, i * 4 + 4);
				res.adjustOrPutValue(k, v, v);
			}
		}
	}

	private int byteArrayToInt(TByteArrayList cached, int i) {
		return cached.get(i + 3) & 0xFF | (cached.get(i + 2) & 0xFF) << 8
				| (cached.get(i + 1) & 0xFF) << 16
				| (cached.get(i) & 0xFF) << 24;
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
