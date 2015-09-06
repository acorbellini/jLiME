package edu.jlime.rpc.data;

public interface DataListener {

	public void messageReceived(DataMessage data, Response rsp);
}
