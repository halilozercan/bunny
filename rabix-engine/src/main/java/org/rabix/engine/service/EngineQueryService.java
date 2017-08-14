package org.rabix.engine.service;

import org.rabix.bindings.model.Job;
import org.rabix.engine.service.impl.Query;
import org.rabix.transport.backend.Backend;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by intern-sdk on 9.8.17..
 */
public interface EngineQueryService {

    Set<Job> getJobs(Query query);

    Job getJob(String id);

    void getJobStats(String id);

    Set<Backend> getBackends(Query query);

    Backend getBackend(String id);
}
