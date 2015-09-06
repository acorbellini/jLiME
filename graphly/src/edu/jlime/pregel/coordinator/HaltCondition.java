package edu.jlime.pregel.coordinator;

import java.io.Serializable;

public interface HaltCondition extends Serializable {

	public boolean eval(CoordinatorTask coordinatorTask, int step);

}
