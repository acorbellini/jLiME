package edu.jlime.graphly.rec;

import java.util.Map;

import edu.jlime.graphly.client.Graphly;

public interface Update {
	public Map<String, Object> exec(Long vid, Graphly g) throws Exception;
}
