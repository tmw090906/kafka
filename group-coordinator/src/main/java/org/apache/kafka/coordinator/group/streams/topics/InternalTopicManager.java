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
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TopicMetadata;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalTopicManager {


    public static ConfiguredTopology configureTopics(LogContext logContext,
                                                     StreamsTopology topology,
                                                     Map<String, TopicMetadata> topicMetadata) {
        final Logger log = logContext.logger(InternalTopicManager.class);
        final Collection<StreamsGroupTopologyValue.Subtopology> subtopologies = topology.subtopologies().values();

        final Map<String, ConfiguredSubtopology> configuredSubtopologies =
            subtopologies.stream()
                .collect(Collectors.toMap(
                    StreamsGroupTopologyValue.Subtopology::subtopologyId,
                    InternalTopicManager::fromPersistedSubtopology)
                );

        final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology =
            subtopologies.stream()
                .collect(Collectors.toMap(
                    StreamsGroupTopologyValue.Subtopology::subtopologyId,
                    InternalTopicManager::copartitionGroupsFromPersistedSubtopology)
                );

        final Map<String, ConfiguredInternalTopic> configuredInternalTopics =
            configuredSubtopologies.values().stream().flatMap(x ->
                Stream.concat(
                    x.repartitionSourceTopics().values().stream(),
                    x.stateChangelogTopics().values().stream()
                )
            ).collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x));

        final Function<String, Integer> topicPartitionCountProvider =
            topic -> getPartitionCount(topicMetadata, topic, configuredInternalTopics);

        Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();
        try {
            configureRepartitionTopics(logContext, configuredSubtopologies, topicPartitionCountProvider);
            enforceCopartitioning(logContext, configuredSubtopologies, copartitionGroupsBySubtopology, topicPartitionCountProvider, log);
            configureChangelogTopics(logContext, configuredSubtopologies, topicPartitionCountProvider);
        } catch (TopicConfigurationException e) {
            log.debug("Error configuring topics for topology {}: {}", topology.topologyEpoch(), e.toString());
            topicConfigurationException = Optional.of(e);
        }

        Map<String, CreatableTopic> internalTopicsToCreate;
        if (topicConfigurationException.isEmpty()) {
            try {
                internalTopicsToCreate = missingTopics(configuredSubtopologies, topicMetadata);
                if (!internalTopicsToCreate.isEmpty()) {
                    topicConfigurationException = Optional.of(TopicConfigurationException.missingInternalTopics(
                        "Internal topics are missing: " + internalTopicsToCreate.keySet()
                    ));
                }
            } catch (TopicConfigurationException e) {
                log.debug("Error configuring topics for topology {}: {}", topology.topologyEpoch(), e.toString());
                topicConfigurationException = Optional.of(e);
                internalTopicsToCreate = Collections.emptyMap();
            }
        } else {
            internalTopicsToCreate = Collections.emptyMap();
        }

        if (topicConfigurationException.isEmpty()) {
            log.info("Valid topic configuration found, topology {} is now initialized.", topology.topologyEpoch());
        } else {
            log.info("Topic configuration incomplete, topology {} is not ready: {} ", topology.topologyEpoch(),
                topicConfigurationException.toString());
        }

        return new ConfiguredTopology(
            topology.topologyEpoch(),
            configuredSubtopologies,
            internalTopicsToCreate,
            topicConfigurationException
        );
    }


    private static Map<String, CreatableTopic> missingTopics(Map<String, ConfiguredSubtopology> subtopologyMap,
                                                             Map<String, TopicMetadata> topicMetadata) {

        final Map<String, CreatableTopic> topicsToCreate = new HashMap<>();
        for (ConfiguredSubtopology subtopology : subtopologyMap.values()) {
            subtopology.repartitionSourceTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
            subtopology.stateChangelogTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
        }
        for (Map.Entry<String, TopicMetadata> topic : topicMetadata.entrySet()) {
            final TopicMetadata existingTopic = topic.getValue();
            final CreatableTopic expectedTopic = topicsToCreate.remove(topic.getKey());
            if (expectedTopic != null) {
                if (existingTopic.numPartitions() != expectedTopic.numPartitions()) {
                    throw TopicConfigurationException.incorrectlyPartitionedTopics("Existing topic " + topic.getKey() + " has different"
                        + " number of partitions: expected " + expectedTopic.numPartitions() + ", found " + existingTopic.numPartitions());
                }
            }
        }
        return topicsToCreate;
    }

    private static void configureRepartitionTopics(LogContext logContext,
                                                   Map<String, ConfiguredSubtopology> configuredSubtopologies,
                                                   Function<String, Integer> topicPartitionCountProvider) {
        final RepartitionTopics repartitionTopics = new RepartitionTopics(logContext,
            configuredSubtopologies,
            topicPartitionCountProvider);
        repartitionTopics.setup();
    }

    private static void enforceCopartitioning(LogContext logContext,
                                              Map<String, ConfiguredSubtopology> configuredSubtopologies,
                                              Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology,
                                              Function<String, Integer> topicPartitionCountProvider,
                                              Logger log) {
        final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer = new CopartitionedTopicsEnforcer(
            logContext, topicPartitionCountProvider);

        final Map<String, ConfiguredInternalTopic> repartitionTopicConfigs =
            configuredSubtopologies.values().stream().flatMap(x ->
                x.repartitionSourceTopics().values().stream()
            ).collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x));

        if (repartitionTopicConfigs.isEmpty()) {
            log.info("Skipping the repartition topic validation since there are no repartition topics.");
        } else {
            // ensure the co-partitioning topics within the group have the same number of partitions,
            // and enforce the number of partitions for those repartition topics to be the same if they
            // are co-partitioned as well.
            for (Collection<Set<String>> copartitionGroups : copartitionGroupsBySubtopology.values()) {
                for (Set<String> copartitionGroup : copartitionGroups) {
                    copartitionedTopicsEnforcer.enforce(copartitionGroup, repartitionTopicConfigs);
                }
            }
        }
    }

    private static void configureChangelogTopics(LogContext logContext,
                                                 Map<String, ConfiguredSubtopology> configuredSubtopologies,
                                                 Function<String, Integer> topicPartitionCountProvider) {
        final ChangelogTopics changelogTopics = new ChangelogTopics(logContext,
            configuredSubtopologies, topicPartitionCountProvider);
        changelogTopics.setup();
    }

    private static Integer getPartitionCount(Map<String, TopicMetadata> topicMetadata,
                                             String topic,
                                             Map<String, ConfiguredInternalTopic> configuredInternalTopics) {
        final TopicMetadata metadata = topicMetadata.get(topic);
        if (metadata == null) {
            if (configuredInternalTopics.containsKey(topic) && configuredInternalTopics.get(topic).numberOfPartitions().isPresent()) {
                return configuredInternalTopics.get(topic).numberOfPartitions().get();
            } else {
                return null;
            }
        } else {
            return metadata.numPartitions();
        }
    }

    private static CreatableTopic toCreatableTopic(final ConfiguredInternalTopic config) {

        final CreatableTopic creatableTopic = new CreatableTopic();

        creatableTopic.setName(config.name());

        if (config.numberOfPartitions().isEmpty()) {
            throw new IllegalStateException("Number of partitions must be set for topic " + config.name());
        } else {
            creatableTopic.setNumPartitions(config.numberOfPartitions().get());
        }

        if (config.replicationFactor().isPresent() && config.replicationFactor().get() != 0) {
            creatableTopic.setReplicationFactor(config.replicationFactor().get());
        } else {
            creatableTopic.setReplicationFactor((short) -1);
        }

        final CreatableTopicConfigCollection topicConfigs = new CreatableTopicConfigCollection();

        config.topicConfigs().forEach((k, v) -> {
            final CreatableTopicConfig topicConfig = new CreatableTopicConfig();
            topicConfig.setName(k);
            topicConfig.setValue(v);
            topicConfigs.add(topicConfig);
        });

        creatableTopic.setConfigs(topicConfigs);

        return creatableTopic;
    }

    private static ConfiguredSubtopology fromPersistedSubtopology(
        final StreamsGroupTopologyValue.Subtopology subtopology) {
        // TODO: Need to resolve regular expressions here.
        return new ConfiguredSubtopology(
            new HashSet<>(subtopology.repartitionSinkTopics()),
            new HashSet<>(subtopology.sourceTopics()),
            subtopology.repartitionSourceTopics().stream()
                .map(InternalTopicManager::fromPersistedTopicInfo)
                .collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x)),
            subtopology.stateChangelogTopics().stream()
                .map(InternalTopicManager::fromPersistedTopicInfo)
                .collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x))
        );
    }

    private static ConfiguredInternalTopic fromPersistedTopicInfo(
        final StreamsGroupTopologyValue.TopicInfo topicInfo) {
        return new ConfiguredInternalTopic(
            topicInfo.name(),
            topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                .collect(Collectors.toMap(StreamsGroupTopologyValue.TopicConfig::key,
                    StreamsGroupTopologyValue.TopicConfig::value))
                : Collections.emptyMap(),
            topicInfo.partitions() == 0 ? Optional.empty() : Optional.of(topicInfo.partitions()),
            topicInfo.replicationFactor() == 0 ? Optional.empty()
                : Optional.of(topicInfo.replicationFactor()));
    }

    private static Collection<Set<String>> copartitionGroupsFromPersistedSubtopology(
        final StreamsGroupTopologyValue.Subtopology subtopology) {
        return subtopology.copartitionGroups().stream().map(copartitionGroup ->
            Stream.concat(
                copartitionGroup.sourceTopics().stream()
                    .map(i -> subtopology.sourceTopics().get(i)),
                copartitionGroup.repartitionSourceTopics().stream()
                    .map(i -> subtopology.repartitionSourceTopics().get(i).name())
            ).collect(Collectors.toSet())
        ).collect(Collectors.toList());
    }
}
