package edu.jlime.graphly.rec;

import java.util.Map.Entry;
import java.util.NavigableSet;

import com.google.common.collect.TreeMultimap;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class CircleOfTrust implements CustomFunction {

	private int steps;
	private float max;
	private int top;

	public CircleOfTrust(int steps, float max_depth, int top) {
		this.steps = steps;
		this.max = max_depth;
		this.top = top;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		Graphly g = tr.getGraph();

		TraversalResult cot = g.v(before.vertices().toArray())
				.set("mapper", tr.get("mapper")).as(Recommendation.class)
				.randomwalk("circleoftrust", steps, max, Dir.OUT).exec();

		TLongObjectHashMap<Object> c = g.collect("circleoftrust", -1, cot
				.vertices().toArray());

		TLongFloatHashMap ret = new TLongFloatHashMap();

		TreeMultimap<Float, Long> sorted = TreeMultimap.create();
		TLongObjectIterator<Object> collectIt = c.iterator();
		while (collectIt.hasNext()) {
			collectIt.advance();
			TLongFloatHashMap properties = (TLongFloatHashMap) collectIt
					.value();
			TLongFloatIterator it = properties.iterator();
			while (it.hasNext()) {
				it.advance();
				float value = it.value();
				if (sorted.size() < top) {
					sorted.put(value, it.key());
				} else {
					Float toRemove = sorted.asMap().firstKey();
					if (toRemove.compareTo(value) < 0) {
						NavigableSet<Long> navigableSet = sorted.get(toRemove);
						Long f = navigableSet.first();
						navigableSet.remove(f);
						sorted.put(value, it.key());
					}
				}
			}

			// ret.putAll(properties);
		}
		for (Entry<Float, Long> l : sorted.entries()) {
			ret.put(l.getValue(), l.getKey());
		}

		return new CountResult(ret);
	}
}
