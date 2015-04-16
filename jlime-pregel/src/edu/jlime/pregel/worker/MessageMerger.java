package edu.jlime.pregel.worker;

import java.io.Serializable;

public interface MessageMerger extends Serializable {

	// public void merge(PregelMessage msg1, PregelMessage msg2, PregelMessage
	// into);

	MessageQueueFactory getFactory();

}
