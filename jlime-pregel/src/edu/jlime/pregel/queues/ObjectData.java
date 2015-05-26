package edu.jlime.pregel.queues;

public class ObjectData {

	public Object[] objects;
	public long[] vids;

	int dCont = 0;
	int vCont = 0;

	public ObjectData(int i) {
		this.vids = new long[i];
		this.objects = new Object[i];
	}

	public void addL(long to) {
		vids[vCont++] = to;
	}

	public void addObj(Object value) {
		objects[dCont++] = value;
	}

}
