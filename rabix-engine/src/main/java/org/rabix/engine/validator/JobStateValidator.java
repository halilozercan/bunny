package org.rabix.engine.validator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rabix.engine.model.JobRecord;
import org.rabix.engine.service.impl.JobRecordServiceImpl.JobState;

public class JobStateValidator {
  
  private static Map<JobState, List<JobState>> transitions = new HashMap<JobState, List<JobState>>();

  static {
    List<JobState> transitionFromPending = new ArrayList<JobState>();
    transitionFromPending.add(JobState.READY);
    transitionFromPending.add(JobState.COMPLETED);
    transitions.put(JobState.PENDING, transitionFromPending);
    List<JobState> transitionFromReady = new ArrayList<JobState>();
    transitionFromReady.add(JobState.RUNNING);
    transitionFromReady.add(JobState.FAILED);
    transitionFromReady.add(JobState.COMPLETED);
    transitions.put(JobState.READY, transitionFromReady);
    List<JobState> transitionFromRunning = new ArrayList<JobState>();
    transitionFromRunning.add(JobState.COMPLETED);
    transitionFromRunning.add(JobState.ABORTED);
    transitionFromRunning.add(JobState.FAILED);
    transitions.put(JobState.RUNNING, transitionFromRunning);
    List<JobState> transitionFromCompleted = new ArrayList<JobState>();
    transitionFromCompleted.add(JobState.READY);
    transitions.put(JobState.COMPLETED, transitionFromCompleted);
    List<JobState> transitionFromFailed = new ArrayList<JobState>();
    transitions.put(JobState.FAILED, transitionFromFailed);
    List<JobState> transitionFromAborted = new ArrayList<JobState>();
    transitionFromAborted.add(JobState.COMPLETED);
    transitions.put(JobState.ABORTED, transitionFromAborted);
    
    transitions = Collections.unmodifiableMap(transitions);
  }
  
  public static JobState checkState(JobRecord jobRecord, JobState jobState) throws JobStateValidationException {
    return checkState(jobRecord.getState(), jobState);
  }
  
  public static JobState checkState(JobState currentState, JobState jobState) throws JobStateValidationException {
    if (transitions.get(currentState).contains(jobState)) {
      return jobState;
    } else {
      throw new JobStateValidationException("Job state cannot transition from " + currentState + " to " + jobState);
    }
  }

}
