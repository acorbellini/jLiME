package edu.jlime.pregel.coordinator;

import java.io.Serializable;

public interface Aggregator extends Serializable {

	public void superstep(int s);

	public Aggregator copy();

	public void reset();

	public void merge(Aggregator value);
}
