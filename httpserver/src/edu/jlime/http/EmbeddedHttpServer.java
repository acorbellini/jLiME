package edu.jlime.http;

import java.net.URI;

import org.apache.log4j.Logger;
import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.FragmentConfiguration;
import org.eclipse.jetty.webapp.MetaInfConfiguration;
import org.eclipse.jetty.webapp.TagLibConfiguration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebInfConfiguration;
import org.eclipse.jetty.webapp.WebXmlConfiguration;

import edu.jlime.jd.DEFExtension;
import edu.jlime.jd.JobCluster;

public class EmbeddedHttpServer implements DEFExtension {

	public EmbeddedHttpServer() {
		log.info(getClass().getClassLoader());
	}

	Logger log = Logger.getLogger(EmbeddedHttpServer.class);

	Server server;

	// public static class HttpServerInfoProv extends InfoProvider {
	// private EmbeddedHttpServer server;
	//
	// public HttpServerInfoProv(EmbeddedHttpServer server) {
	// this.server = server;
	// }

	// @Override
	// public DEFInfo getInfo() {
	// DEFInfo info = new DEFInfo("httpserver");
	// info.put("url", server.getServer());
	// return info;
	// }
	// }

	public void start(final JobCluster c) throws Exception {
		new Thread() {
			public void run() {
				boolean started = false;
				int portCont = 0;
				while (!started & portCont <= 1000) {

					server = new Server();
					ServerConnector conn = new ServerConnector(server, 2, 2);
					conn.setPort(3330 + portCont);
					server.addConnector(conn);
					WebAppContext monitor = new WebAppContext();
					monitor.setContextPath("/");
					// String dir =
					// "../DEFMonitorWebapp/target/def-monitor-webapp/";
					// monitor.setResourceBase(dir);
					monitor.setResourceBase("extra/webmonitor/web");
					monitor.setDescriptor("extra/webmonitor/web/WEB-INF/web.xml");
					monitor.setConfigurations(new Configuration[] {
							new AnnotationConfiguration(),
							new WebXmlConfiguration(),
							new WebInfConfiguration(),
							new TagLibConfiguration(),
							new MetaInfConfiguration(),
							new FragmentConfiguration() });
					monitor.getServletContext().setAttribute("cluster", c);
					server.setHandler(monitor);

					try {
						server.start();
						started = true;
						log.info("Succesfully started http server on port "
								+ (3330 + portCont));
					} catch (Exception e) {
					}
					portCont++;
				}
				try {
					server.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			};
		}.start();
	}

	public URI getServer() {
		return server.getURI();
	}

	public static void main(String[] args) throws Exception {
		new EmbeddedHttpServer().start(null);
	}
}
