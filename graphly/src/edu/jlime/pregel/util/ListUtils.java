package edu.jlime.pregel.util;

import java.util.List;

import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.ConcatList;

public class ListUtils {

	public static List<PregelMessage> concat(List<PregelMessage> l1, List<PregelMessage> l2) {
		return new ConcatList<PregelMessage>(l1, l2);
	}

}
