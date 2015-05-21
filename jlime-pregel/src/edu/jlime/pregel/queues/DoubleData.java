package edu.jlime.pregel.queues;

public class DoubleData {
	public DoubleData(int size) {
		keys = new long[size];
		values = new double[size];
	}

	public long[] keys;
	public double[] values;
	private int lcont = 0;
	private int fcont = 0;

	public void addL(long to) {
		keys[lcont] = to;
		lcont++;
	}

	public void addF(double f) {
		values[fcont] = f;
		fcont++;
	}
}
