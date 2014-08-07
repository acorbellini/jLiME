package edu.jlime.core.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BroadcastException extends Exception {

	private static final long serialVersionUID = 2392272222976735718L;

	List<Exception> listOfExcep = new ArrayList<>();

	public BroadcastException(String string) {
		super(string);
	}

	public void add(Exception e) {
		List<StackTraceElement> eStack = Arrays.asList(e.getStackTrace());
		ArrayList<StackTraceElement> currentStack = new ArrayList<>(
				Arrays.asList(getStackTrace()));
		currentStack.addAll(eStack);
		setStackTrace(currentStack.toArray(new StackTraceElement[] {}));
		listOfExcep.add(e);
	}

	public List<Exception> getListOfExcep() {
		return listOfExcep;
	}

	@Override
	public String getMessage() {
		String messages = "";
		for (Exception exc : listOfExcep)
			messages += exc.getMessage() + " ";
		return messages;
	}

	public boolean isEmpty() {
		return listOfExcep.isEmpty();
	}

}
