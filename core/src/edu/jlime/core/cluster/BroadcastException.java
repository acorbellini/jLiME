package edu.jlime.core.cluster;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class BroadcastException extends Exception {

	private static final long serialVersionUID = 2392272222976735718L;

	HashMap<Peer, Exception> listOfExcep = new HashMap<>();

	public BroadcastException(String string) {
		super(string);
	}

	public void put(Peer p, Exception e) {
		List<StackTraceElement> eStack = Arrays.asList(e.getStackTrace());
		ArrayList<StackTraceElement> currentStack = new ArrayList<>(
				Arrays.asList(getStackTrace()));
		currentStack.addAll(eStack);
		setStackTrace(currentStack.toArray(new StackTraceElement[] {}));
		listOfExcep.put(p, e);
	}

	public boolean isEmpty() {
		return listOfExcep.isEmpty();
	}

	@Override
	public void printStackTrace(PrintStream s) {
		s.println(getClass().getName() + ": Received (" + listOfExcep.size()
				+ ") Broadcast Exception/s");
		for (Entry<Peer, Exception> e : listOfExcep.entrySet()) {
			s.print("From " + e.getKey() + " = ");
			e.getValue().printStackTrace(s);
		}
	}
}
