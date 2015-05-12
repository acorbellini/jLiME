package edu.jlime.rpc;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Configuration {

	private Properties prop;

	public Configuration(Properties prop) {
		this.prop = prop;
	}

	public Configuration() {
		this(null);
	}

	public boolean getBoolean(String k, boolean defaultValue) {
		if (prop != null && prop.getProperty(k) != null)
			try {
				return new Boolean(prop.getProperty(k));
			} catch (Exception e) {
				e.printStackTrace();
			}
		return defaultValue;
	}

	public float getFloat(String k, float defaultValue) {
		if (prop != null && prop.getProperty(k) != null)
			try {
				return new Float(prop.getProperty(k));
			} catch (Exception e) {
				e.printStackTrace();
			}
		return defaultValue;
	}

	public String getString(String k, String defaultValue) {
		if (prop != null && prop.getProperty(k) != null)
			try {
				return prop.getProperty(k);
			} catch (Exception e) {
				e.printStackTrace();
			}
		return defaultValue;
	}

	public Integer getInt(String k, Integer defaultValue) {
		if (prop != null && prop.getProperty(k) != null)
			try {
				return new Integer(prop.getProperty(k));
			} catch (Exception e) {
				e.printStackTrace();
			}
		return defaultValue;
	}

	public static Configuration newConfig(String propFile) {

		Logger log = Logger.getLogger(NetworkConfiguration.class);

		Properties prop = new Properties();
		if (propFile != null) {
			try {
				prop.load(new FileInputStream(propFile));
			} catch (Exception e) {
				log.error("Could not load " + propFile);
				prop = null;
			}
		}
		return new Configuration(prop);
	}
}
