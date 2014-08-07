package edu.jlime.rpc.fd;

import edu.jlime.rpc.PeerJlime;

public interface FailureListener {

	public void nodeFailed(PeerJlime node);
}
