package edu.jlime.core.rpc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class Wrappers {
	public static HashMap<Class<?>, Class<?>> wrappers = new HashMap<>();

	public static HashMap<Class<?>, String> unwrappers = new HashMap<>();

	static {

		wrappers.put(boolean.class, Boolean.class);
		wrappers.put(int.class, Integer.class);
		wrappers.put(float.class, Float.class);
		wrappers.put(double.class, Double.class);
		wrappers.put(char.class, Character.class);
		wrappers.put(byte.class, Byte.class);
		wrappers.put(long.class, Long.class);
		wrappers.put(short.class, Short.class);
		wrappers.put(void.class, Void.class);

		unwrappers.put(Boolean.class, "booleanValue");
		unwrappers.put(Integer.class, "intValue");
		// unwrappers.put(float.class, Float.class);
		// unwrappers.put(double.class, Double.class);
		// unwrappers.put(char.class, Character.class);
		// unwrappers.put(byte.class, Byte.class);
		// unwrappers.put(long.class, Long.class);
		// unwrappers.put(short.class, Short.class);
		// unwrappers.put(void.class, Void.class);

	}

	public static Class<?> get(Class<?> returnType) {
		return wrappers.get(returnType);
	}

	public static Object unwrap(Object object) {
		Class<?> wrapper = object.getClass();
		String unwrapper = unwrappers.get(wrapper);
		try {
			Method valueOf = wrapper.getMethod(unwrapper);
			return valueOf.invoke(object);
		} catch (NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static Object wrap(Object object) {
		Class<?> wrapper = get(object.getClass());
		try {
			Method valueOf = wrapper.getMethod("valueOf", object.getClass());
			return valueOf.invoke(object, object.getClass());
		} catch (NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}