package edu.jlime.rpc.data;

import edu.jlime.rpc.data.DataProcessor.DataMessage;

public interface DataListener {

	public void messageReceived(DataMessage data, Response rsp);
}
