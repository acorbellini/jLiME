package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;

public interface ClassLoaderProvider {

	Class<?> loadClass(Peer classSource, String className) throws ClassNotFoundException;

}
