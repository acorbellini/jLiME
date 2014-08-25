package edu.jlime.core.rpc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.lang.model.type.NullType;

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

	public Class<?>[] getArgTypes() {
		if (objects.length == 0)
			return new Class<?>[] {};
		Class<?>[] classes = new Class<?>[objects.length];
		for (int i = 0; i < objects.length; i++) {
			if (objects[i] != null)
				classes[i] = objects[i].getClass();
			else
				classes[i] = Object.class;

		}
		return classes;
	}

	public String getName() {
		return name;
	}

	public Object[] getObjects() {
		return objects;
	}

	@Override
	public String toString() {
		return "MethodCall [objectKey=" + objectKey + ", name=" + name
				+ ", objects=" + Arrays.toString(objects) + "]";
	}

	public void unwrapArgument(int i) {
		objects[i] = Wrappers.unwrap(objects[i]);
	}

}
