package edu.jlime.pregel.worker;

import java.util.List;

public class ListUtils {

	public static List<PregelMessage> concat(List<PregelMessage> l1,
			List<PregelMessage> l2) {
		return new ConcatList<PregelMessage>(l1, l2);
	}

}
