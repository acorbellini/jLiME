package edu.jlime.rpc.data;

import edu.jlime.rpc.message.JLiMEAddress;

public interface DataProvider {

	public void addDataListener(DataListener list);

	public byte[] sendData(byte[] msg, JLiMEAddress to, boolean waitForResponse)
			throws Exception;
}
