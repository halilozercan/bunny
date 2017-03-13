package org.rabix.engine.rest;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.rabix.bindings.model.Job;
import org.rabix.common.helper.JSONHelper;

public class ManualTest {

  private static final String URL = "http://localhost" + ":" + 8081 + "/v0/engine/jobs";
  Client client;


  public static void main(String[] args) throws IOException, InterruptedException {
    ManualTest test = new ManualTest();
    Set<Job> jobs = test.postJob(args[0], args[1], Integer.valueOf(args[2]));

    Thread.sleep(1000);
    for (Job j : jobs) {
      System.out.println(j.getId() + " - " +j.getStatus());
      test.abortJob(j);
    }

    for (Job j : jobs) {
      System.out.println(j.getId() + " - " + test.getJob(j).getStatus());
    }
  }

  public ManualTest() {
    client = ClientBuilder.newClient(new ClientConfig());
  }

  public Set<Job> postJob(String jobFile, String inputsFile, int times) throws IOException {
    Map<String, Object> inputs = JSONHelper.readMap(JSONHelper.transformToJSON(FileUtils.readFileToString(new File(inputsFile))));
    Job job = new Job("file:///" + jobFile, inputs);
    WebTarget webTarget = client.target(URL);
    Invocation.Builder invocationBuilder = webTarget.request(javax.ws.rs.core.MediaType.APPLICATION_JSON).header("batch", times);
    Response r = invocationBuilder.post(Entity.entity(job, javax.ws.rs.core.MediaType.APPLICATION_JSON));

    return r.readEntity(new GenericType<Set<Job>>() {});
  }

  public void abortJob(Job j) {
    WebTarget webTarget = client.target(URL + "/" + j.getId() + "/ABORTED");
    Invocation.Builder invocationBuilder = webTarget.request(javax.ws.rs.core.MediaType.APPLICATION_JSON);
    Response r = invocationBuilder.put(Entity.json(""));
  }

  public Job getJob(Job j) {
    WebTarget webTarget = client.target(URL + "/" + j.getId());
    Invocation.Builder invocationBuilder = webTarget.request(javax.ws.rs.core.MediaType.APPLICATION_JSON);
    return invocationBuilder.get(Job.class);
  }
}
