package edu.jlime.graphly.traversal;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.graphly.recommendation.Update;
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
		Map<Long, Map<String, Object>> updates = new HashMap<>();
		// get updates
		TLongIterator it = before.vertices().iterator();
		while (it.hasNext()) {
			long l = it.next();
			Map<String, Object> map = update.exec(l, tr.getGraph());
			updates.put(l, map);
		}
		// commit
		for (Entry<Long, Map<String, Object>> l : updates.entrySet()) {
			tr.getGraph().setProperties(l.getKey(), l.getValue());
		}

		return before;
	}
}
