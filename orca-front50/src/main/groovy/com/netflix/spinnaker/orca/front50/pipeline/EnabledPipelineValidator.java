/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.front50.pipeline;

import com.netflix.spinnaker.orca.ExecutionStatus;
import com.netflix.spinnaker.orca.front50.Front50Service;
import com.netflix.spinnaker.orca.pipeline.PipelineValidator;
import com.netflix.spinnaker.orca.pipeline.model.Pipeline;
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.schedulers.Schedulers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;

@Component
@Slf4j
public class EnabledPipelineValidator implements PipelineValidator {

  private final Front50Service front50Service;
  private ExecutionRepository executionRepository;

  @Autowired
  public EnabledPipelineValidator(Optional<Front50Service> front50Service, ExecutionRepository executionRepository) {
    this.front50Service = front50Service.orElse(null);
    this.executionRepository = executionRepository;
  }


  @Override public void checkRunnable(Pipeline pipeline) {
    if (front50Service == null) {
      throw new UnsupportedOperationException("Front50 not enabled, no way to validate pipeline. Fix this by setting front50.enabled: true");
    }
    List<Map<String, Object>> pipelines = isStrategy(pipeline) ? front50Service.getStrategies(pipeline.getApplication()) : front50Service.getPipelines(pipeline.getApplication());
    pipelines
      .stream()
      .filter(it -> it.get("id").equals(pipeline.getPipelineConfigId()))
      .findFirst()
      .ifPresent(it -> {
        if ((boolean) it.getOrDefault("disabled", false)) {
          throw new PipelineIsDisabled(it.get("id").toString(), it.get("application").toString(), it.get("name").toString());
        }
      });
  }

  @Override public void checkShouldQueue(Pipeline pipeline) {
    if (executionRepository == null) {
      throw new UnsupportedOperationException("Execution repository not available, how you gonna do anything?");
    }
    String pipelineConfigId = pipeline.getPipelineConfigId();
    ExecutionRepository.ExecutionCriteria runningCriteria = new ExecutionRepository.ExecutionCriteria()
            .setLimit(Integer.MAX_VALUE)
            .setStatuses(Collections.singletonList(ExecutionStatus.RUNNING.toString()));
    List<Pipeline> allRunningExecutions = executionRepository.retrievePipelinesForPipelineConfigId(
            pipelineConfigId,
            runningCriteria).subscribeOn(Schedulers.io()).toList().toBlocking().single();
    Predicate<Pipeline> sameTriggerMessageHash = p -> p.getTrigger().get("messageHash").equals(pipeline.getTrigger().get("messageHash"));
    Boolean runningMatch = allRunningExecutions.stream().anyMatch(sameTriggerMessageHash);
    for (Pipeline p : allRunningExecutions) {
      System.out.printf(",, Running execution trigger msg hash: %s\n", p.getTrigger().get("messageHash"));
    }

    List<String> completedStatuses = ExecutionStatus.COMPLETED.stream().map(Enum::toString).collect(Collectors.toList());
    ExecutionRepository.ExecutionCriteria completedCriteria = new ExecutionRepository.ExecutionCriteria()
            .setLimit(Integer.MAX_VALUE)
            .setStatuses(completedStatuses);

    // If configured with Pubsub trigger, check that the pipeline wasn't triggered twice
    // by an identical set of pubsub message artifacts.
    List<Pipeline> allCompletedExecutions = executionRepository.retrievePipelinesForPipelineConfigId(
            pipelineConfigId,
            completedCriteria).subscribeOn(Schedulers.io()).toList().toBlocking().single();
    Boolean completedMatch = allCompletedExecutions.stream().anyMatch(sameTriggerMessageHash);
    for (Pipeline p : allCompletedExecutions) {
      System.out.printf(",, Completed execution trigger msg hash: %s\n", p.getTrigger().get("messageHash"));
    }
    if (runningMatch || completedMatch) {
      throw new DuplicateMessageTrigger(format("Pipeline with id %s failed due to a duplicate message trigger", pipelineConfigId));
    }
  }

  private boolean isStrategy(Pipeline pipeline) {
    Map<String, Object> trigger = pipeline.getTrigger();
    Object strategy = ((Map<String, Object>) trigger.getOrDefault("parameters", emptyMap()))
      .getOrDefault("strategy", false);
    return "pipeline".equals(trigger.get("type")) && Boolean.TRUE.equals(strategy);
  }

  static class PipelineIsDisabled extends PipelineValidationFailed {
    PipelineIsDisabled(String id, String application, String name) {
      super(format("The pipeline config %s (%s) belonging to %s is disabled", name, id, application));
    }
  }

  static class DuplicateMessageTrigger extends PipelineValidationFailed {
    DuplicateMessageTrigger(String message) {
      super(message);
    }
  }
}
