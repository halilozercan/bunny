package org.rabix.engine.event.impl;

import java.util.Map;
import java.util.UUID;

import org.rabix.engine.event.Event;
import org.rabix.engine.service.impl.JobRecordServiceImpl.JobState;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JobStatusEvent implements Event {

  @JsonProperty("jobId")
  private final String jobId;
  @JsonProperty("state")
  private final JobState state;
  @JsonProperty("contextId")
  private final UUID contextId;
  
  @JsonProperty("result")
  private final Map<String, Object> result;
  @JsonProperty("message")
  private final String message;
  
  @JsonProperty("eventGroupId")
  private final UUID eventGroupId;
  @JsonProperty("producedByNode")
  private final String producedByNode;

  public JobStatusEvent(String jobId, UUID contextId, JobState state, UUID eventGroupId, String producedByNode) {
    this.jobId = jobId;
    this.contextId = contextId;
    this.state = state;
    this.result = null;
    this.message = null;
    this.eventGroupId = eventGroupId;
    this.producedByNode = producedByNode;
  }
  
  public JobStatusEvent(String jobId, UUID contextId, JobState state, Map<String, Object> result, UUID eventGroupId, String producedByNode) {
    this.jobId = jobId;
    this.contextId = contextId;
    this.state = state;
    this.result = result;
    this.message = null;
    this.eventGroupId = eventGroupId;
    this.producedByNode = producedByNode;
  }
  
  public JobStatusEvent(String jobId, UUID contextId, JobState state, String message, UUID eventGroupId, String producedByNode) {
    this.jobId = jobId;
    this.contextId = contextId;
    this.state = state;
    this.result = null;
    this.message = message;
    this.eventGroupId = eventGroupId;
    this.producedByNode = producedByNode;
  }
  
  @JsonCreator
  public JobStatusEvent(@JsonProperty("jobId") String jobId, @JsonProperty("state") JobState state,
      @JsonProperty("contextId") UUID contextId, @JsonProperty("result") Map<String, Object> result, @JsonProperty("message") String message,
      @JsonProperty("eventGroupId") UUID eventGroupId, @JsonProperty("producedByNode") String producedByNode) {
    this.jobId = jobId;
    this.state = state;
    this.contextId = contextId;
    this.message = message;
    this.result = result;
    this.eventGroupId = eventGroupId;
    this.producedByNode = producedByNode;
  }

  public String getJobId() {
    return jobId;
  }
  
  public JobState getState() {
    return state;
  }

  @Override
  public UUID getContextId() {
    return contextId;
  }
  
  public Map<String, Object> getResult() {
    return result;
  }
  
  @Override
  public UUID getEventGroupId() {
    return eventGroupId;
  }
  
  @Override
  public String getProducedByNode() {
    return producedByNode;
  }
  
  @Override
  public EventType getType() {
    return EventType.JOB_STATUS_UPDATE;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((contextId == null) ? 0 : contextId.hashCode());
    result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
    result = prime * result + ((this.result == null) ? 0 : this.result.hashCode());
    result = prime * result + ((state == null) ? 0 : state.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JobStatusEvent other = (JobStatusEvent) obj;
    if (contextId == null) {
      if (other.contextId != null)
        return false;
    } else if (!contextId.equals(other.contextId))
      return false;
    if (jobId == null) {
      if (other.jobId != null)
        return false;
    } else if (!jobId.equals(other.jobId))
      return false;
    if (result == null) {
      if (other.result != null)
        return false;
    } else if (!result.equals(other.result))
      return false;
    if (state != other.state)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "JobStatusEvent [jobId=" + jobId + ", state=" + state + ", contextId=" + contextId + ", result=" + result + "]";
  }

  @Override
  public PersistentEventType getPersistentType() {
    switch (state) {
    case RUNNING:
      return PersistentEventType.JOB_STATUS_UPDATE_RUNNING;
    case COMPLETED:
      return PersistentEventType.JOB_STATUS_UPDATE_COMPLETED;
    default:
      break;
    }
    return null;
  }

}
