package edu.jlime.graphly.http;

import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.util.logging.LogManager;

import javax.ws.rs.core.UriBuilder;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.net.httpserver.HttpServer;

public class WebServer {

	private static final int PORT_RANGE = 10;
	private int port = 8080;

	public WebServer() {
		LogManager.getLogManager().reset();
		SLF4JBridgeHandler.install();
	}

	public WebServer port(int p) {
		this.port = p;
		return this;
	}

	private HttpServer httpserver() throws IllegalArgumentException,
			IOException {
		ResourceConfig config = new PackagesResourceConfig(
				"edu.jlime.graphly.http.resource");
		return HttpServerFactory.create(getURI(getPort()), config);
	}

	private static URI getURI(int port) {
		return UriBuilder.fromUri("http://localhost/").port(port).build();
	}

	public static void main(String[] args) throws IllegalArgumentException,
			IOException {
		new WebServer().start();
	}

	public void start() throws IllegalArgumentException, IOException {
		int cont = 0;
		HttpServer httpserver = null;
		while (true)
			try {
				httpserver = httpserver();
				httpserver.start();
				return;
			} catch (BindException e) {
				if (httpserver != null)
					httpserver.stop(0);
				if (cont == PORT_RANGE)
					throw e;
				port++;
				cont++;
			}
	}

	public int getPort() {
		return port;
	}
}
