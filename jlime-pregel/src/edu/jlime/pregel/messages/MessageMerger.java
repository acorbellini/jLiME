package edu.jlime.pregel.messages;

import java.io.Serializable;

import edu.jlime.pregel.queues.MessageQueueFactory;

public interface MessageMerger extends Serializable {

	// public void merge(PregelMessage msg1, PregelMessage msg2, PregelMessage
	// into);

	MessageQueueFactory getFactory();

}
