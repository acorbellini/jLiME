package edu.jlime.metrics.metric;

import java.io.Serializable;

public interface Metric<T> extends Serializable {

	public void update(T val);
}
