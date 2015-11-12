package edu.jlime.pregel.worker;

public class FloatData {
	public FloatData(int size) {
		keys = new long[size];
		values = new float[size];
	}

	public FloatData(long[] keys2, float[] values2) {
		this.keys = keys2;
		this.values = values2;
	}

	public long[] keys;
	public float[] values;
	private int lcont = 0;
	private int fcont = 0;

	public void addL(long to) {
		keys[lcont] = to;
		lcont++;
	}

	public void addF(float f) {
		values[fcont] = f;
		fcont++;
	}

}
