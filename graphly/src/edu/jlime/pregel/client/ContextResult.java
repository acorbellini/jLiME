package edu.jlime.pregel.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

public class ContextResult {

	private List<HashMap<String, TLongFloatHashMap>> result = new ArrayList<>();
	private TObjectFloatHashMap<String> broadcast;
	private TObjectFloatHashMap<Pair<String, String>> sg_broadcast;
	private HashMap<String, Aggregator> aggs;

	public ContextResult(HashMap<String, TLongFloatHashMap> result,
			TObjectFloatHashMap<String> broadcast,
			TObjectFloatHashMap<Pair<String, String>> sg_broadcast,
			HashMap<String, Aggregator> aggs) {
		this.result.add(result);
		this.broadcast = broadcast;
		this.sg_broadcast = sg_broadcast;
		this.aggs = aggs;
	}

	public void mergeWith(ContextResult ctx, PregelConfig c) {
		this.result.addAll(ctx.result);

		for (Entry<String, Aggregator> e : ctx.getAggs().entrySet()) {
			Aggregator current = this.aggs.get(e.getKey());
			if (current == null)
				this.aggs.put(e.getKey(), e.getValue());
			else
				current.merge(e.getValue());
		}

		TObjectFloatIterator<String> it = ctx.broadcast.iterator();
		while (it.hasNext()) {
			it.advance();
			c.getMerger(it.key()).merge(it.key(), it.value(), broadcast);
		}

		TObjectFloatIterator<Pair<String, String>> it_SG = ctx.sg_broadcast
				.iterator();
		while (it_SG.hasNext()) {
			it_SG.advance();
			c.getMerger(it_SG.key().left).merge(it_SG.key(), it_SG.value(),
					sg_broadcast);
		}
	}

	public List<HashMap<String, TLongFloatHashMap>> getResult() {
		return result;
	}

	public TObjectFloatHashMap<String> getBroadcast() {
		return broadcast;
	}

	public TObjectFloatHashMap<Pair<String, String>> getSg_broadcast() {
		return sg_broadcast;
	}

	public HashMap<String, Aggregator> getAggs() {
		return aggs;
	}
}
