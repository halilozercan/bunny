package org.rabix.engine.rest.api;

import java.util.UUID;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.transport.backend.Backend;
import org.skife.jdbi.v2.sqlobject.customizers.QueryTimeOut;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v1")
public interface JobHTTPService {

  @POST
  Response create(Job job, @HeaderParam("batch") Integer batch);
  
  @GET
  @Path("/jobs/{id}")
  Response get(@PathParam("id")  UUID id);

  @GET
  @Path("/jobs")
  Response jobs(@Context UriInfo ui);

  @PUT
  @Path("/{id}/{status}")
  Response update(@PathParam("id") UUID id, @PathParam("status") JobStatus status);

  @GET
  @Path("/backends")
  Response backends(@QueryParam("name") String name,
                    @QueryParam("status") Backend.BackendStatus status,
                    @QueryParam("type") Backend.BackendType type);
  
}
