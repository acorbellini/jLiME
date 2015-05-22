package edu.jlime.graphly.client;

import java.util.List;

public class GatherResult<T> {
	private List<T> val;

	public GatherResult(List<T> gather) {
		this.val = gather;
	}

	public List<T> value() {
		return val;
	}

	T merge(GatherMerger<T> merger) {
		return merger.merge(val);
	}
}
