/*
 * Copyright 2019 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.front50.spring


import com.netflix.spinnaker.fiat.shared.FiatStatus
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.extensionpoint.pipeline.PipelinePreprocessor
import com.netflix.spinnaker.orca.front50.DependentPipelineStarter
import com.netflix.spinnaker.orca.front50.Front50Service
import com.netflix.spinnaker.orca.listeners.ExecutionListener
import com.netflix.spinnaker.orca.listeners.Persister
import com.netflix.spinnaker.orca.pipeline.model.Execution
import com.netflix.spinnaker.orca.pipeline.util.ContextParameterProcessor
import com.netflix.spinnaker.orca.pipelinetemplate.V2Util
import com.netflix.spinnaker.security.User
import groovy.transform.CompileDynamic
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import static com.netflix.spinnaker.orca.pipeline.model.Execution.ExecutionType.PIPELINE

@Slf4j
@CompileDynamic
@Component
class DependentPipelineExecutionListener implements ExecutionListener {

  @Autowired
  private final Front50Service front50Service

  @Autowired
  private final DependentPipelineStarter dependentPipelineStarter

  @Autowired
  private final FiatStatus fiatStatus

  @Autowired(required = false)
  private final List<PipelinePreprocessor> pipelinePreprocessors

  @Autowired
  private final ContextParameterProcessor contextParameterProcessor

  DependentPipelineExecutionListener(Front50Service front50Service,
                                     DependentPipelineStarter dependentPipelineStarter,
                                     FiatStatus fiatStatus,
                                     List<PipelinePreprocessor> pipelinePreprocessor,
                                     ContextParameterProcessor contextParameterProcessor) {
    this.front50Service = front50Service
    this.dependentPipelineStarter = dependentPipelineStarter
    this.fiatStatus = fiatStatus
    this.pipelinePreprocessors = pipelinePreprocessor
    this.contextParameterProcessor = contextParameterProcessor
  }

  @Override
  void afterExecution(Persister persister, Execution execution, ExecutionStatus executionStatus, boolean wasSuccessful) {
    if (!execution || !(execution.type == PIPELINE)) {
      return
    }

    def status = convertStatus(execution)
    def allPipelines = front50Service.getAllPipelines()
    if (pipelinePreprocessors) {
      // Resolve templated pipelines if enabled.
      allPipelines = allPipelines.findAll { it.type == 'templatedPipeline'}
      .collect { pipeline ->
        V2Util.planPipeline(contextParameterProcessor, pipelinePreprocessors, pipeline)
//        pipeline.put("plan", true) // avoid resolving artifacts
//        for (PipelinePreprocessor pp : pipelinePreprocessors) {
//          pipeline = pp.process(pipeline)
//        }
//
//        Map<String, Object> augmentedContext = new HashMap<>()
//        augmentedContext.put("trigger", pipeline.get("trigger"))
//        augmentedContext.put("templateVariables", pipeline.getOrDefault("templateVariables", Collections.EMPTY_MAP))
//        return contextParameterProcessor.process(pipeline, augmentedContext, false)
      }
    }

    allPipelines.findAll { !it.disabled }
      .each {
      it.triggers.each { trigger ->
        if (trigger.enabled &&
          trigger.type == 'pipeline' &&
          trigger.pipeline &&
          trigger.pipeline == execution.pipelineConfigId &&
          trigger.status.contains(status)
        ) {
          User authenticatedUser = null

          if (fiatStatus.enabled && trigger.runAsUser) {
            authenticatedUser = new User()
            authenticatedUser.setEmail(trigger.runAsUser)
          }

          dependentPipelineStarter.trigger(
            it,
            execution.trigger?.user as String,
            execution,
            [:],
            null,
            authenticatedUser
          )
        }
      }
    }
  }

  private static String convertStatus(Execution execution) {
    switch (execution.status) {
      case ExecutionStatus.CANCELED:
        return 'canceled'
        break
      case ExecutionStatus.SUSPENDED:
        return 'suspended'
        break
      case ExecutionStatus.SUCCEEDED:
        return 'successful'
        break
      default:
        return 'failed'
    }
  }

  @Override
  int getOrder() {
    return HIGHEST_PRECEDENCE
  }
}
