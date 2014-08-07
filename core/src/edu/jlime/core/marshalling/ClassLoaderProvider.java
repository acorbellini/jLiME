package edu.jlime.core.marshalling;

public interface ClassLoaderProvider {

	Class<?> loadClass(String classSource, String className)
			throws ClassNotFoundException;

}
