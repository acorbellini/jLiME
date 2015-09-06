package edu.jlime.jd.mapreduce;

import java.util.ArrayList;

import edu.jlime.core.cluster.BroadcastException;

public class MapReduceException extends BroadcastException {

	private static final long serialVersionUID = -4820966918884097707L;

	ArrayList<Object> subresults = new ArrayList<>();

	public MapReduceException() {
		super("Map Reduce Exception");
	}

	public void addSubRes(Object obj) {
		subresults.add(obj);
	}

	public ArrayList<Object> getSubresults() {
		return subresults;
	}
}
