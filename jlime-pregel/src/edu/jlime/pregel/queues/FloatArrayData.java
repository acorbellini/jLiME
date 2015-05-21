package edu.jlime.pregel.queues;

public class FloatArrayData {
	long[] vids;
	float[][] data;
	int cont = 0;
	int data_cont = 0;

	public FloatArrayData(int i) {
		this.vids = new long[i];
		this.data = new float[i][];
	}

	public void addL(long to) {
		vids[cont++] = to;
	}

	public void addF(float[] value) {
		data[data_cont++] = value;
	}

	public long[] getVids() {
		return vids;
	}

	public float[][] getData() {
		return data;
	}

}
