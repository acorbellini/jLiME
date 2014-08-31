package edu.jlime.pregel.coordinator;

import java.io.Serializable;

import edu.jlime.pregel.graph.Vertex;

public interface Aggregator extends Serializable {
	public Double getVal(Vertex v);

	public void setVal(Vertex v, Double value);

	public void superstep(int s);
}
