package edu.jlime.core.rpc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MethodCall implements Serializable {

	private static final long serialVersionUID = -8541391959402389105L;

	private String objectKey;

	private String name;

	private Object[] objects;

	public MethodCall(String k, String name, Object[] objects) {
		this.objectKey = k;
		this.objects = objects;
		this.name = name;
	}

	public String getObjectKey() {
		return objectKey;
	}

	public List<Class<?>> getArgTypes() {
		List<Class<?>> classes = new ArrayList<Class<?>>();
		for (Object o : objects) {
			classes.add(o.getClass());
		}
		return classes;
	}

	public String getName() {
		return name;
	}

	public Object[] getObjects() {
		return objects;
	}

}
