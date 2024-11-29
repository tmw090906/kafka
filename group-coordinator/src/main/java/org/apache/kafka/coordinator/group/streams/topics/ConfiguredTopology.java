/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Immutable configured topology metadata.
 */
public class ConfiguredTopology {

    private final int topologyEpoch;

    private final Map<String, ConfiguredSubtopology> subtopologies;

    private final Map<String, CreatableTopic> internalTopicsToBeCreated;

    private final Optional<TopicConfigurationException> topicConfigurationException;

    public ConfiguredTopology(final int topologyEpoch,
                              final Map<String, ConfiguredSubtopology> subtopologies,
                              final Map<String, CreatableTopic> internalTopicsToBeCreated,
                              final Optional<TopicConfigurationException> topicConfigurationException
    ) {
        this.topologyEpoch = topologyEpoch;
        this.subtopologies = subtopologies;
        this.internalTopicsToBeCreated = internalTopicsToBeCreated;
        this.topicConfigurationException = topicConfigurationException;
    }

    public int topologyEpoch() {
        return topologyEpoch;
    }

    public Map<String, ConfiguredSubtopology> subtopologies() {
        return subtopologies;
    }

    public Map<String, CreatableTopic> internalTopicsToBeCreated() {
        return internalTopicsToBeCreated;
    }

    public Optional<TopicConfigurationException> topicConfigurationException() {
        return topicConfigurationException;
    }

    public boolean isReady() {
        return topicConfigurationException.isEmpty();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConfiguredTopology that = (ConfiguredTopology) o;
        return Objects.equals(topologyEpoch, that.topologyEpoch) && Objects.equals(subtopologies, that.subtopologies)
            && Objects.equals(internalTopicsToBeCreated, that.internalTopicsToBeCreated) && Objects.equals(
            topicConfigurationException, that.topicConfigurationException);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topologyEpoch, subtopologies, internalTopicsToBeCreated, topicConfigurationException);
    }

    @Override
    public String toString() {
        return "ConfiguredTopology{" +
            "topologyEpoch='" + topologyEpoch + '\'' +
            ", subtopologies=" + subtopologies +
            ", internalTopicsToBeCreated=" + internalTopicsToBeCreated +
            ", topicConfigurationException=" + topicConfigurationException +
            '}';
    }

    public StreamsGroupDescribeResponseData.Topology asStreamsGroupDescribeTopology() {
        return new StreamsGroupDescribeResponseData.Topology()
            .setEpoch(topologyEpoch)
            .setSubtopologies(subtopologies.entrySet().stream().map(
                subtopology -> new StreamsGroupDescribeResponseData.Subtopology()
                    .setSubtopologyId(subtopology.getKey())
                    .setSourceTopics(subtopology.getValue().sourceTopics().stream().sorted().collect(Collectors.toList()))
                    .setRepartitionSinkTopics(subtopology.getValue().repartitionSinkTopics().stream().sorted().collect(Collectors.toList()))
                    .setRepartitionSourceTopics(
                        asStreamsGroupDescribeTopicInfo(subtopology.getValue().repartitionSourceTopics().values()))
                    .setStateChangelogTopics(
                        asStreamsGroupDescribeTopicInfo(subtopology.getValue().stateChangelogTopics().values()))
            ).collect(Collectors.toList()));
    }

    private static List<StreamsGroupDescribeResponseData.TopicInfo> asStreamsGroupDescribeTopicInfo(
        final Collection<ConfiguredInternalTopic> topicInfos) {
        return topicInfos.stream().map(x ->
            new StreamsGroupDescribeResponseData.TopicInfo()
                .setName(x.name())
                .setPartitions(x.numberOfPartitions().orElse(0))
                .setReplicationFactor(x.replicationFactor().orElse((short) 0))
                .setTopicConfigs(
                    x.topicConfigs() != null ?
                        x.topicConfigs().entrySet().stream().map(
                            y -> new StreamsGroupDescribeResponseData.KeyValue()
                                .setKey(y.getKey())
                                .setValue(y.getValue())
                        ).collect(Collectors.toList()) : null
                )
        ).sorted().collect(Collectors.toList());
    }
}
