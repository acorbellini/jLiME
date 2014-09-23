package edu.jlime.pregel.coordinator;

import java.io.Serializable;

public interface Aggregator extends Serializable {
	public Double getVal(long v);

	public void setVal(long v, Double value);

	public void superstep(int s);
}
