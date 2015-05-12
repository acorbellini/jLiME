package edu.jlime.graphly.http;

import java.io.IOException;
import java.net.URI;
import java.util.logging.LogManager;

import javax.ws.rs.core.UriBuilder;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.net.httpserver.HttpServer;

public class WebServer {
	private HttpServer httpserver() throws IllegalArgumentException,
			IOException {
		ResourceConfig config = new PackagesResourceConfig(
				"edu.jlime.graphly.http.web");
		return HttpServerFactory.create(getURI(), config);
	}

	private static URI getURI() {
		return UriBuilder.fromUri("http://localhost/").port(8085).build();
	}

	public static void main(String[] args) throws IllegalArgumentException,
			IOException {
		LogManager.getLogManager().reset();
		SLF4JBridgeHandler.install();

		new WebServer().start();
	}

	private void start() throws IllegalArgumentException, IOException {
		httpserver().start();
	}
}
