package org.rabix.engine.stub.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.CommandLine;
import org.rabix.bindings.helper.FileValueHelper;
import org.rabix.bindings.mapper.FileMappingException;
import org.rabix.bindings.mapper.FilePathMapper;
import org.rabix.bindings.model.DirectoryValue;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.bindings.model.requirement.DockerContainerRequirement;
import org.rabix.bindings.model.requirement.EnvironmentVariableRequirement;
import org.rabix.bindings.model.requirement.FileRequirement;
import org.rabix.bindings.model.requirement.FileRequirement.SingleFileRequirement;
import org.rabix.bindings.model.requirement.FileRequirement.SingleInputDirectoryRequirement;
import org.rabix.bindings.model.requirement.FileRequirement.SingleInputFileRequirement;
import org.rabix.bindings.model.requirement.FileRequirement.SingleTextFileRequirement;
import org.rabix.bindings.model.requirement.Requirement;
import org.rabix.common.helper.ChecksumHelper.HashAlgorithm;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.JobServiceException;
import org.rabix.engine.stub.BackendStub;
import org.rabix.engine.tes.model.TESDockerExecutor;
import org.rabix.engine.tes.model.TESJob;
import org.rabix.engine.tes.model.TESResources;
import org.rabix.engine.tes.model.TESState;
import org.rabix.engine.tes.model.TESTask;
import org.rabix.engine.tes.model.TESTaskParameter;
import org.rabix.engine.tes.model.TESVolume;
import org.rabix.transport.backend.impl.BackendLocal;
import org.rabix.transport.mechanism.TransportPlugin.ErrorCallback;
import org.rabix.transport.mechanism.TransportPlugin.ReceiveCallback;
import org.rabix.transport.mechanism.TransportPlugin.ResultPair;
import org.rabix.transport.mechanism.TransportPluginException;
import org.rabix.transport.mechanism.impl.tes.TransportPluginTes;
import org.rabix.transport.mechanism.impl.tes.TransportQueueTes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendStubTes extends BackendStub<TransportQueueTes, BackendLocal, TransportPluginTes> {

  public static final String HOME_ENV_VAR = "HOME";
  public static final String TMPDIR_ENV_VAR = "TMPDIR";

  static String workingDirTes = "/mnt/working_dir/";
  static String inputsTes = "/mnt/inputs/";
  Logger logger = LoggerFactory.getLogger(BackendStubTes.class);
  private Configuration configuration;

  public BackendStubTes(JobService jobService, Configuration configuration, BackendLocal backendLocal) throws TransportPluginException {
    this.jobService = jobService;
    this.backend = backendLocal;
    this.transportPlugin = new TransportPluginTes(configuration);

    this.sendToBackendQueue = new TransportQueueTes(backendLocal.getToBackendQueue());
    // this.sendToBackendControlQueue = new
    // TransportQueueTes(backendLocal.getToBackendControlQueue());
    // this.receiveFromBackendQueue = new TransportQueueTes(backendLocal.getFromBackendQueue());
    // this.receiveFromBackendHeartbeatQueue = new
    // TransportQueueTes(backendLocal.getFromBackendHeartbeatQueue());

    this.enableControlMesages = configuration.getBoolean("engine.enable_backend_control_messages", true);
    this.configuration = configuration;
  }

  @Override
  public void start(org.rabix.engine.stub.BackendStub.HeartbeatCallback heartbeatCallback, ReceiveCallback<Job> receiveCallback, ErrorCallback errorCallback) {
    // TODO Auto-generated method stub
    // super.start(heartbeatCallback, receiveCallback, errorCallback);
  }

  private void stageInput(String name, Object input, List<TESTaskParameter> inputs) {
    if (!(input instanceof FileValue)) {
      if (input instanceof List) {
        List list = (List) input;
        for (int i = 0; i < list.size(); i++) {
          stageInput(name + i, list.get(i), inputs);
        }
      }
      return;
    }
    FileValue file = (FileValue) input;
    inputs.add(
        new TESTaskParameter(name, "", "file://" + file.getPath(), inputsTes + file.getPath(), file instanceof DirectoryValue ? "Directory" : "File", true));
    if (file.getSecondaryFiles() != null)
      for (int i = 0; i < file.getSecondaryFiles().size(); i++) {
        stageInput(name + i, file.getSecondaryFiles().get(i), inputs);
      }
  }

  @Override
  public void send(Object message) {
    if (!(message instanceof Job))
      return;
    Job job = (Job) message;
    Integer minimumCpuCores = 1;
    boolean preemptible = false;
    Double minimumRamGb = 1d;
    String rootDir = configuration.getString("backend.execution.directory");
    File workingDir = new File(rootDir + "/" + job.getRootId() + "/" + job.getName().replace(".", "/"));
    workingDir.mkdir();
    String workPath = "file://" + workingDir.getAbsolutePath() + "/";

    FilePathMapper filePathMapper = (String path, Map<String, Object> config) -> path.startsWith("/") ? path : rootDir + "/" + path;
    FilePathMapper tesMapper = (String path, Map<String, Object> config) -> path.startsWith("/mnt") ? path : inputsTes + path;

    String[] zones = new String[] {};
    List<TESVolume> volumes = new ArrayList<>();
    List<TESTaskParameter> inputs = new ArrayList<>();
    List<TESTaskParameter> outputs = new ArrayList<>();
    List<TESDockerExecutor> dockerExecutors = new ArrayList<>();
    volumes.add(new TESVolume("working_dir", 1d, null, workingDirTes, false));
    volumes.add(new TESVolume("inputs", 1d, null, inputsTes, false));
    TESResources resources = new TESResources(minimumCpuCores, preemptible, minimumRamGb, volumes, zones);
    try {

      Bindings bindings = BindingsFactory.create(job);
      if (bindings.isSelfExecutable(job)) {
        job = FileValueHelper.mapInputFilePaths(job, filePathMapper);
        job = bindings.preprocess(job, workingDir, filePathMapper);
        job = bindings.postprocess(job, workingDir, HashAlgorithm.SHA1, filePathMapper);
        try {
          jobService.update(Job.cloneWithStatus(job, JobStatus.COMPLETED));
        } catch (JobServiceException e1) {
          e1.printStackTrace();
        }
        return;
      }

      job = FileValueHelper.mapInputFilePaths(job, filePathMapper);
      job = bindings.preprocess(job, workingDir, filePathMapper);

      List<Requirement> combinedRequirements = new ArrayList<>();
      combinedRequirements.addAll(bindings.getHints(job));
      combinedRequirements.addAll(bindings.getRequirements(job));
      stageFileRequirements(workingDir, combinedRequirements);

      FileRequirement requirement = getRequirement(combinedRequirements, FileRequirement.class);
      if (requirement != null) {
        inputs.addAll(requirement.getFileRequirements().stream().map(r -> {
          return new TESTaskParameter(r.getFilename(), "", workPath + r.getFilename(), workingDirTes + r.getFilename(), "File", true);
        }).collect(Collectors.toList()));
      }
      job.getInputs().entrySet().stream().forEach(e -> {
        Object v = e.getValue();
        if (v instanceof Map) {
          for (Entry<String, Object> o : ((Map<String, Object>) v).entrySet()) {
            stageInput(o.getKey(), o.getValue(), inputs);
          }
        } else {
          stageInput(e.getKey(), v, inputs);
        }
      });

      job = FileValueHelper.mapInputFilePaths(job, tesMapper);
      CommandLine cmdLine = bindings.buildCommandLineObject(job, new File("/mnt/working_dir/"), tesMapper);

      List<String> commandLine = new ArrayList<String>();
      // if (!Arrays.asList("/bin/sh","/bin/sh -c","/bin/bash","/bin/bash -c",
      // "python").contains(cmdLine.getParts().get(0))) {
      // commandLine.add("/bin/sh");
      // commandLine.add("-c");
      // }
      List<String> parts = cmdLine.getParts();
      if (parts.stream().anyMatch(p -> p.contains(" "))) {
        commandLine.add("/bin/sh");
        commandLine.add("-c");
        commandLine.add(cmdLine.build());
      } else {
        commandLine.addAll(parts);
      }
      outputs.add(new TESTaskParameter("directory", "", workPath, workingDirTes, "Directory", true));
      String dockerPull = "rfranklin/pythondev";
      DockerContainerRequirement docker = getRequirement(combinedRequirements, DockerContainerRequirement.class);
      if (docker != null) {
        dockerPull = docker.getDockerPull();
      }

      EnvironmentVariableRequirement env = getRequirement(combinedRequirements, EnvironmentVariableRequirement.class);
      Map<String, String> variables = env == null ? new HashMap<>() : env.getVariables();

      dockerExecutors.add(new TESDockerExecutor(dockerPull, commandLine, workingDirTes,
          StringUtils.isEmpty(cmdLine.getStandardIn()) ? null
              : cmdLine.getStandardIn().startsWith("/mnt/") ? cmdLine.getStandardIn() : inputsTes + cmdLine.getStandardIn(),
          StringUtils.isEmpty(cmdLine.getStandardOut()) ? null
              : cmdLine.getStandardOut().startsWith(workingDirTes) ? cmdLine.getStandardOut() : workingDirTes + cmdLine.getStandardOut(),
          StringUtils.isEmpty(cmdLine.getStandardError()) ? null
              : cmdLine.getStandardError().startsWith(workingDirTes) ? cmdLine.getStandardError() : workingDirTes + cmdLine.getStandardError(),
          variables));

    } catch (BindingException e) {
      e.printStackTrace();
      try {
        jobService.update(Job.cloneWithStatus(job, JobStatus.FAILED));
      } catch (JobServiceException e1) {
        e1.printStackTrace();
      }
      return;
    }


    TESTask tesTask = new TESTask(job.getName(), job.getRootId().toString(), "", inputs, outputs, resources, job.getRootId().toString(), dockerExecutors);
    final Job finalJob = job;
    ResultPair<TESTask> send = this.transportPlugin.send(sendToBackendQueue, tesTask);
    if (send.isSuccess()) {
      String response = send.getMessage();
      TransportQueueTes taskId = new TransportQueueTes(response);
      transportPlugin.startReceiver(taskId, TESJob.class, new ReceiveCallback<TESJob>() {

        @Override
        public void handleReceive(TESJob entity) throws TransportPluginException {
          if (entity.getState().equals(TESState.Complete)) {
            logger.debug("COMPLETE");
            try {
              Job tempJob = BindingsFactory.create(finalJob).postprocess(finalJob, workingDir, HashAlgorithm.SHA1, new FilePathMapper() {

                @Override
                public String map(String path, Map<String, Object> config) throws FileMappingException {
                  // TODO Auto-generated method stub
                  return path;
                }
              });

              jobService.update(Job.cloneWithStatus(tempJob, JobStatus.COMPLETED));
            } catch (BindingException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } catch (JobServiceException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } finally {
              transportPlugin.stopReceiver(taskId);
            }
          }
          if (entity.getState().equals(TESState.Error)) {
            try {
              jobService.update(Job.cloneWithStatus(finalJob, JobStatus.FAILED));
            } catch (JobServiceException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }

      }, new ErrorCallback() {

        @Override
        public void handleError(Exception error) {
          // TODO Auto-generated method stub

        }
      });
    }

  }

  private void stageFileRequirements(File workingDir, List<Requirement> requirements) throws BindingException {
    try {
      FileRequirement fileRequirementResource = getRequirement(requirements, FileRequirement.class);
      if (fileRequirementResource == null) {
        return;
      }

      List<SingleFileRequirement> fileRequirements = fileRequirementResource.getFileRequirements();
      if (fileRequirements == null) {
        return;
      }
      for (SingleFileRequirement fileRequirement : fileRequirements) {
        logger.info("Process file requirement {}", fileRequirement);

        File destinationFile = new File(workingDir, fileRequirement.getFilename());
        if (fileRequirement instanceof SingleTextFileRequirement) {
          FileUtils.writeStringToFile(destinationFile, ((SingleTextFileRequirement) fileRequirement).getContent());
          continue;
        }
        if (fileRequirement instanceof SingleInputFileRequirement || fileRequirement instanceof SingleInputDirectoryRequirement) {
          String path = ((SingleInputFileRequirement) fileRequirement).getContent().getPath();
          File file = new File(path);
          if (!file.exists()) {
            continue;
          }
          if (file.isFile()) {
            FileUtils.copyFile(file, destinationFile);
          } else {
            FileUtils.copyDirectory(file, destinationFile);
          }
        }
      }
    } catch (IOException e) {
      logger.error("Failed to process file requirements.", e);
      throw new BindingException("Failed to process file requirements.");
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Requirement> T getRequirement(List<Requirement> requirements, Class<T> clazz) {
    for (Requirement requirement : requirements) {
      if (requirement.getClass().equals(clazz)) {
        return (T) requirement;
      }
    }
    return null;
  }

}
