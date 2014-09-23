package edu.jlime.core.cluster;

import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class BroadcastException extends Exception {

	private static final long serialVersionUID = 2392272222976735718L;

	ConcurrentHashMap<Peer, Exception> listOfExcep = new ConcurrentHashMap<>();

	public BroadcastException(String string) {
		super(string);
	}

	public Map<Peer, Exception> getListOfExcep() {
		return listOfExcep;
	}

	public void put(Peer p, Exception e) {
		// List<StackTraceElement> eStack = Arrays.asList(e.getStackTrace());
		// ArrayList<StackTraceElement> currentStack = new ArrayList<>(
		// Arrays.asList(getStackTrace()));
		// currentStack.addAll(eStack);
		// setStackTrace(currentStack.toArray(new StackTraceElement[] {}));
		
		if(listOfExcep.containsKey(p))
			System.out.println("Exception from  " + p + " already existed.");
		
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
