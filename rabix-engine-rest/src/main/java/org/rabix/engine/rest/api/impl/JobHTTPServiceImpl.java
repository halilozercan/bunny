package org.rabix.engine.rest.api.impl;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.engine.rest.api.JobHTTPService;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.JobServiceException;

import com.google.inject.Inject;
import org.rabix.engine.store.repository.JobRepository;
import org.slf4j.LoggerFactory;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/engine/jobs")
public class JobHTTPServiceImpl implements JobHTTPService {

  private final JobService jobService;
  private final JobRepository jobRepository;

  @Inject
  public JobHTTPServiceImpl(JobService jobService, JobRepository jobRepository) {
    this.jobService = jobService;
    this.jobRepository = jobRepository;
  }

  @Override
  @POST
  public Response create(Job job, @HeaderParam("batch") Integer batch) {
    try {
      if (batch != null) {
        for (int i=0;i<batch;i++) {
          jobService.start(job, null);
        }
        return ok("success");
      }
      return ok(jobService.start(job, null));
    } catch (Exception e) {
      LoggerFactory.getLogger(JobHTTPServiceImpl.class).error(e.getMessage());
      return error();
    }
  }
  
  @Override
  @GET
  @Path("/{id}")
  public Response get(@PathParam("id") UUID id) {
    Job job = jobService.get(id);
    if (job == null) {
      return entityNotFound();
    }
    return ok(job);
  }

  @Override
  @PUT
  @Path("/{id}")
  public Response save(@PathParam("id") UUID id, Job job) {
    try {
      jobService.update(job);
    } catch (JobServiceException e) {
      return error();
    }
    return ok();
  }
  
  @Override
  @PUT
  @Path("/{id}/{status}")
  public Response update(@PathParam("id") UUID id, @PathParam("status") JobStatus status){
    try {
      Job job = jobService.get(id);
      job = Job.cloneWithStatus(job, status);

      jobService.update(job);
    } catch (JobServiceException e) {
      return error();
    }
    return ok();
  }

  @Override
  @GET
  @Path("/query")
  public Response query(@QueryParam("status") JobStatus status,
                        @QueryParam("id") UUID id,
                        @QueryParam("app") String appGlob,
                        @QueryParam("name") String name) {
    Set<Job> jobs = new HashSet<>();
    Set<Job> allJobs = jobRepository.get();
    for (Job job : allJobs) {
      if (status != null && job.getStatus() != status) {
        continue;
      }
      else if (id != null && !job.getId().equals(id)) {
        continue;
      }
      else if (name!=null && !job.getName().equals(name)) {
        continue;
      }
      else if (appGlob != null) {
        // Glob matching for app
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + appGlob);
        if (matcher.matches(Paths.get(job.getApp()))) {
          continue;
        }
      }
      jobs.add(job);
    }
    if (jobs.size() == 0) {
      return entityNotFound();
    }
    return ok(jobs);
  }

  private Response entityNotFound() {
    return Response.status(Status.NOT_FOUND).build();
  }
  
  private Response error() {
    return Response.status(Status.BAD_REQUEST).build();
  }
  
  private Response ok() {
    return Response.ok(Collections.emptyMap()).build();
  }
  
  private Response ok(Object items) {
    if (items == null) {
      return ok();
    }
    return Response.ok().entity(items).build();
  }

}
