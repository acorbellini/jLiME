package edu.jlime.graphly.traversal;

import java.io.Serializable;

import org.apache.log4j.Logger;

import edu.jlime.jd.task.ResultListener;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.hash.TLongFloatHashMap;

public class CountMerger implements
		ResultListener<TraversalResult, TraversalResult>, Serializable {
	TLongFloatHashMap temp = new TLongFloatHashMap();

	@Override
	public void onSuccess(TraversalResult result) {
		Logger log = Logger.getLogger(CountMerger.class);

		CountResult res = (CountResult) result;

		log.info("Received result with " + res.size() + " vertices.");

		synchronized (temp) {
			TLongFloatIterator it = res.iterator();

			while (it.hasNext()) {
				it.advance();
				long key = it.key();
				float value = it.value();
				temp.adjustOrPutValue(key, value, value);
			}
		}
		log.info("Finished adding to result.");
	}

	@Override
	public TraversalResult onFinished() {
		return new CountResult(temp);
	}

	@Override
	public void onFailure(Exception res) {
	}

}
