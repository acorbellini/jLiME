package edu.jlime.metrics.metric;

import java.util.HashSet;
import java.util.Set;

public abstract class TimedSet implements SensorMeasure {

	Set<String> last = new HashSet<>();

	@Override
	public void proc(Metrics mgr) throws Exception {
		Set<String> newSet = updateSet(mgr);
		last.removeAll(newSet);
		for (String removed : last) {
			mgr.deleteAll(removed);
		}
		last = newSet;
	}

	public abstract Set<String> updateSet(IMetrics mgr) throws Exception;

}
