package edu.jlime.pregel.worker;

public interface Worker {
	public void sendDataToVertex(int vertexid, byte[] data) throws Exception;

	public void nextSuperstep(int superstep) throws Exception;
}
