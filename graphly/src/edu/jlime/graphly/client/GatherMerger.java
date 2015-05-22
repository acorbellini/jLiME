package edu.jlime.graphly.client;

import java.io.Serializable;
import java.util.List;

public interface GatherMerger<T> extends Serializable {
	public T merge(List<T> merge);
}
