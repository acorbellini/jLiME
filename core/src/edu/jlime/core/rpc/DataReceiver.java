package edu.jlime.core.rpc;

public interface DataReceiver {

	public byte[] process(String origin, byte[] data);
}
