package edu.jlime.graphly.traversal;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.jlime.graphly.rec.Update;
import gnu.trove.iterator.TLongIterator;

public class UpdateStep implements Step {

	private Update update;
	private GraphlyTraversal tr;

	public UpdateStep(Update update, GraphlyTraversal tr) {
		this.update = update;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		// ExecutorService exec = Executors.newFixedThreadPool(8);
		final Map<Long, Map<String, Object>> updates = new ConcurrentHashMap<>();
		// get updates
		TLongIterator it = before.vertices().iterator();
		while (it.hasNext()) {
			final long l = it.next();
			// exec.execute(new Runnable() {
			// @Override
			// public void run() {
			// try {
			Map<String, Object> map = update.exec(l, tr.getGraph());
			updates.put(l, map);
			// } catch (Exception e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			//
			// }
			// });
		}

		// exec.shutdown();
		// exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		// commit
		for (Entry<Long, Map<String, Object>> l : updates.entrySet()) {
			tr.getGraph().setProperties(l.getKey(), l.getValue());
		}

		return before;
	}
}
