package edu.jlime.collections.adjacencygraph.query;

import java.io.EOFException;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

class CountStreamJob extends StreamJob {

	private static final int READ_BUFFER_SIZE = 32 * 1024;
	private String map;

	public CountStreamJob(String map) {
		this.map = map;
	}

	@Override
	public void run(RemoteInputStream input, RemoteOutputStream outputStream,
			JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(CountStreamJob.class);

		TIntArrayList data = new TIntArrayList();

		log.info("CountStreamJob: Reading input data.");
		try {
			byte[] buffer = new byte[READ_BUFFER_SIZE];
			int read = 0;
			while ((read = input.read(buffer)) != -1)
				for (int i = 0; i < read / 4; i++) {
					data.add(DataTypeUtils.byteArrayToInt(buffer, i * 4));
				}
		} catch (EOFException e) {

		} catch (Exception e) {
			e.printStackTrace();
		}

		log.info("CountStreamJob: Finished reading input stream.");

		log.info("CountStreamJob: Counting " + data.size() + " users.");

		PersistentIntIntArrayMap dkvs = PersistentIntIntArrayMap.getMap(map,
				ctx);

		TIntIntHashMap adyacents = null;
		try {
			log.info("CountStreamJob: Calling DKVS get.");
			adyacents = dkvs.countLists(data.toArray());
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		log.info("CountStreamJob: Finished calling DKVS get, obtained "
				+ adyacents.size());

		log.info("CountStreamJob: Converting obtained hash to bytearray");
		byte[] ret = new byte[4 * 2 * adyacents.size()];
		TIntIntIterator it = adyacents.iterator();
		int pos = 0;
		while (it.hasNext()) {
			it.advance();
			DataTypeUtils.intToByteArray(it.key(), pos * 4, ret);
			DataTypeUtils.intToByteArray(it.value(), pos * 4 + 4, ret);
			pos += 2;
		}
		adyacents.clear();
		log.info("CountStreamJob: Sending obtained count");
		outputStream.write(ret);
		log.info("CountStreamJob: Finished sending obtained count");
		outputStream.close();
	}
}