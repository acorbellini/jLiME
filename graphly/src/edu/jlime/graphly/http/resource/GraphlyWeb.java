package edu.jlime.graphly.http.resource;

import java.io.InputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("")
public class GraphlyWeb {

	@GET
	@Produces(MediaType.TEXT_HTML)
	@Path("/")
	public Response index() {
		return web("");
	}

	@GET
	@Produces(MediaType.TEXT_HTML)
	@Path("/{path:.*}")
	public Response web(@PathParam("path") String path) {
		if (path == null || path.isEmpty())
			path = "index.html";
		InputStream is = getClass().getResourceAsStream("../web/" + path);
		return Response.status(200).entity(is).build();
	}
}
