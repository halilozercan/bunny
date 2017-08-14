package org.rabix.engine.rest.api.impl;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.engine.rest.api.JobHTTPService;
import org.rabix.engine.service.BackendService;
import org.rabix.engine.service.EngineQueryService;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.JobServiceException;
import org.rabix.engine.service.impl.Query;
import org.rabix.transport.backend.Backend;

import javax.ws.rs.GET;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.util.*;

public class JobHTTPServiceImpl implements JobHTTPService {

  private final JobService jobService;
  private final EngineQueryService engineQueryService;
  private final BackendService backendService;

  @Inject
  public JobHTTPServiceImpl(JobService jobService, EngineQueryService engineQueryService,
                            BackendService backendService) {
    this.jobService = jobService;
    this.engineQueryService = engineQueryService;
    this.backendService = backendService;
  }
  
  @Override
  public Response create(Job job, Integer batch) {
    try {
      if (batch != null) {
        for (int i=0;i<batch;i++) {
          jobService.start(job, null);
        }
        return ok("success");
      }
      return ok(jobService.start(job, null));
    } catch (Exception e) {
      return error();
    }
  }
  
  @Override
  public Response get(UUID id) {
    Job job = jobService.get(id);
    if (job == null) {
      return entityNotFound();
    }
    return ok(job);
  }

  @Override
  @GET
  public Response jobs(@Context UriInfo ui) {
    MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
    Query query = new Query();
    for (Map.Entry<String, List<String>> param : queryParams.entrySet()) {
      if (param.getKey().equals("rootId")) {
        UUID value = UUID.fromString(param.getValue().get(0));
        Query.Field<UUID> rootId = new Query.Field<>("rootId", value);
        query.addField(rootId);
      } else if (param.getKey().equals("parentId")) {
        UUID value = UUID.fromString(param.getValue().get(0));
        Query.Field<UUID> parentId = new Query.Field<>("parentId", value);
        query.addField(parentId);
      } else if (param.getKey().equals("name")) {
        String value = String.valueOf(param.getValue().get(0));
        Query.Field<String> name = new Query.Field<>("name", value);
        query.addField(name);
      } else if (param.getKey().startsWith("config.")) {
        String value = String.valueOf(param.getValue().get(0));
        Query.Field<String> config = new Query.Field<>(
                param.getKey().replaceAll("config\\.", ""), value
        );
        query.addField(config);
      } else if (param.getKey().equals("status")) {
        JobStatus value = JobStatus.valueOf(param.getValue().get(0));
        Query.Field<JobStatus> status = new Query.Field<>("status", value);
        query.addField(status);
      } else if (param.getKey().equals("view")) {
        if (param.getValue().get(0).toLowerCase().equals("minimal")) {
          query.setView(Query.View.MINIMAL);
        } else if (param.getValue().get(0).toLowerCase().equals("full")) {
          query.setView(Query.View.FULL);
        }
      }
    }
    Set<Job> jobs = engineQueryService.getJobs(query);

    if (query.getView() == Query.View.MINIMAL) {
      return ok(jobs, Job.MinimalJobView.class);
    } else {
      return ok(jobs);
    }
  }
  
  @Override
  public Response update(UUID id, JobStatus status) {
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
  public Response backends(String name, Backend.BackendStatus status, Backend.BackendType type) {
    List<Backend> backends = backendService.getAllBackends();
    List<Backend> result = new ArrayList<>();
    for(Backend backend:backends) {
      if(name == null || backend.getName().equals(name)) {
        result.add(backend);
      }
      else if(status == null || status.equals(backend.getStatus())) {
        result.add(backend);
      }
      else if(type == null || type.equals(backend.getType())) {
        result.add(backend);
      }
    }
    return ok(result);
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

  private Response ok(Object items, Class view) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.disable(MapperFeature.DEFAULT_VIEW_INCLUSION);

    try {
      String result = mapper
              .writerWithView(view)
              .writeValueAsString(items);

      return Response.ok().entity(result).type(MediaType.APPLICATION_JSON).build();
    } catch(Exception e) {
      return error();
    }
  }

}
