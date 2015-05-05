package edu.jlime.pregel.worker;

import java.util.HashMap;

import edu.jlime.pregel.worker.rpc.Worker;

public interface FloatSenderCallback {

	public HashMap<Worker, FloatData> buildMap() throws Exception;

}
