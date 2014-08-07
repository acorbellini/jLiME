package edu.jlime.rpc.data;

import edu.jlime.rpc.message.Address;

public interface DataProvider {

	public void addDataListener(DataListener list);

	public byte[] sendData(byte[] msg, Address to, boolean waitForResponse)
			throws Exception;
}
