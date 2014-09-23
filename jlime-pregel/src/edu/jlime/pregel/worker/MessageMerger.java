package edu.jlime.pregel.worker;

import java.io.Serializable;

public interface MessageMerger extends Serializable {

	public Object merge(Object v, Object v2);

}
