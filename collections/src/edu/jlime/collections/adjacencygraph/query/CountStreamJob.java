package edu.jlime.collections.adjacencygraph.query;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.util.IntUtils;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

class CountStreamJob extends StreamJob {

	private String map;

	public CountStreamJob(String map) {
		this.map = map;
	}

	@Override
	public void run(RemoteInputStream input, RemoteOutputStream outputStream,
			JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(CountStreamJob.class);

		TIntArrayList data = new TIntArrayList();

		// BufferedInputStream input = new BufferedInputStream(inputStream,
		// 128 * 1024);
		log.info("Reading data.");
		try {
			byte[] buffer = new byte[32 * 1024];
			int read = 0;
			while ((read = input.read(buffer)) != -1)
				for (int i = 0; i < read / 4; i++) {
					data.add(IntUtils.byteArrayToInt(buffer, i * 4));
				}
		} catch (EOFException e) {
			log.info("Finished reading input stream.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		log.info("Counting " + data.size() + " users.");

		PersistentIntIntArrayMap dkvs = PersistentIntIntArrayMap.getMap(map,
				ctx);

		TIntIntHashMap adyacents = null;
		try {
			log.info("Calling DKVS get.");
			adyacents = dkvs.countLists(data.toArray());
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		log.info("Finished calling DKVS get, obtained " + adyacents.size());

		// BufferedOutputStream out = new BufferedOutputStream(outputStream);

		byte[] ret = new byte[4 * 2 * adyacents.size()];
		TIntIntIterator it = adyacents.iterator();
		int pos = 0;
		while (it.hasNext()) {
			it.advance();
			IntUtils.intToByteArray(it.key(), pos * 4, ret);
			IntUtils.intToByteArray(it.value(), pos * 4 + 4, ret);
			pos += 2;
		}
		adyacents.clear();
		outputStream.write(ret);
		outputStream.close();
	}
}