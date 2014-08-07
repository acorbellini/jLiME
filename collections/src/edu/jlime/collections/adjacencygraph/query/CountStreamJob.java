package edu.jlime.collections.adjacencygraph.query;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.job.StreamJob;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

class CountStreamJob extends StreamJob {

	private String map;

	public CountStreamJob(String map) {
		this.map = map;
	}

	@Override
	public void run(RemoteInputStream inputStream,
			RemoteOutputStream outputStream, JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(CountStreamJob.class);

		TIntArrayList data = new TIntArrayList();

		DataInputStream input = RemoteInputStream.getBDIS(inputStream, 4096);
		log.info("Reading data.");
		try {
			while (true) {
				data.add(input.readInt());
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

		DataOutputStream out = RemoteOutputStream.getBDOS(outputStream, 4096);

		for (int k : adyacents.keys()) {
			out.writeInt(k);
			out.writeInt(adyacents.get(k));
		}
		out.close();
	}
}