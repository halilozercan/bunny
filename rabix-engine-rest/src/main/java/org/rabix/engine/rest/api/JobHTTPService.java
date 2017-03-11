package org.rabix.engine.rest.api;

import java.util.UUID;

import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;

public interface JobHTTPService {

  public Response create(Job job, @HeaderParam("batch") Integer batch);
  
  public Response save(@PathParam("id") UUID id, Job job);
  
  public Response get(@PathParam("id")  UUID id);

  public Response update(@PathParam("id") UUID id, @PathParam("status") JobStatus status);
  
}
