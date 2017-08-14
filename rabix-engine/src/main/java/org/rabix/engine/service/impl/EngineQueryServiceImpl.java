package org.rabix.engine.service.impl;

import com.google.inject.Inject;
import javafx.util.Pair;
import org.apache.commons.configuration.Configuration;
import org.rabix.bindings.model.Job;
import org.rabix.engine.service.EngineQueryService;
import org.rabix.engine.service.JobRecordService;
import org.rabix.engine.service.JobStatsRecordService;
import org.rabix.engine.store.repository.JobRecordRepository;
import org.rabix.engine.store.repository.JobRepository;
import org.rabix.transport.backend.Backend;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class EngineQueryServiceImpl implements EngineQueryService {

    private final Configuration configuration;
    private final JobRepository jobRepository;
    private final JobRecordService jobRecordService;
    private final JobRecordRepository jobRecordRepository;
    private final JobStatsRecordService jobStatsRecordService;

    @Inject
    public EngineQueryServiceImpl(JobRecordRepository jobRecordRepository, JobStatsRecordService jobStatsRecordService,
                                  JobRecordService jobRecordService, JobRepository jobRepository,
                                  Configuration configuration) {
        this.configuration = configuration;
        this.jobRepository = jobRepository;
        this.jobRecordService = jobRecordService;
        this.jobRecordRepository = jobRecordRepository;
        this.jobStatsRecordService = jobStatsRecordService;
    }

    @Override
    public Set<Job> getJobs(Query query) {
        Set<Job> currentJobs = this.jobRepository.get();
        for(Query.Field<?> field : query.getFields()) {
            currentJobs = filter(currentJobs, field);
        }
        //TODO: Add view control
        return currentJobs;
    }

    private <T> Set<Job> filter(Set<Job> currentJobs, Query.Field<T> field) {
        Set<Job> result = new LinkedHashSet<>();
        for(Job job:currentJobs) {
            if (field.getName().equals("status") &&
                    job.getStatus().equals(field.getValue())) {
                result.add(job);
            } else if (field.getName().equals("rootId") &&
                    job.getRootId().equals(field.getValue())) {
                result.add(job);
            } else if (field.getName().equals("name") &&
                    job.getName()!=null &&
                    job.getName().equals(field.getValue())) {
                result.add(job);
            } else if (field.getName().equals("parentId") &&
                    job.getParentId()!=null &&
                    job.getParentId().equals(field.getValue())) {
                result.add(job);
            } else if (job.getConfig()!=null &&
                    job.getConfig().containsKey(field.getName()) &&
                    job.getConfig().get(field.getName()).equals(field.getValue())) {
                result.add(job);
            }
        }
        return result;
    }

    @Override
    public Job getJob(String id) {
        return null;
    }

    @Override
    public void getJobStats(String id) {

    }

    @Override
    public Set<Backend> getBackends(Query query) {
        return null;
    }

    @Override
    public Backend getBackend(String id) {
        return null;
    }
}
