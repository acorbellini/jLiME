package edu.jlime.core.rpc;

import java.io.Serializable;

public class MethodCall implements Serializable {

	private static final long serialVersionUID = -8541391959402389105L;

	private String objectKey;

	private String name;

	private Object[] objects;

	private Class<?>[] types;

	public MethodCall(String k, String name, Object[] objects, Class<?>[] types) {
		this.objectKey = k;
		this.objects = objects;
		this.name = name;
		this.types = types;
	}

	public MethodCall(String k, String name, Object[] objects) {
		this(k, name, objects, getClassTypes(objects));
	}

	private static Class<?>[] getClassTypes(Object[] objects) {
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

	public String getObjectKey() {
		return objectKey;
	}

	public Class<?>[] getArgTypes() {
		return types;
	}

	public String getName() {
		return name;
	}

	public Object[] getObjects() {
		return objects;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (Object object : objects) {
			String obj = object.toString();
			if (obj.length() > 100)
				obj = obj.substring(0, 100) + "...";
			builder.append((builder.length() > 0 ? "," : "") + obj);
		}
		return "MethodCall [objectKey=" + objectKey + ", name=" + name + ", objects=" + builder.toString() + "]";
	}

	public void unwrapArgument(int i) {
		objects[i] = Wrappers.unwrap(objects[i]);
	}

}
