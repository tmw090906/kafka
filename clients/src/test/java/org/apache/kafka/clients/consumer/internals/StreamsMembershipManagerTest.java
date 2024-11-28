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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackNeededEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamsMembershipManagerTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;

    private static final String SUB_TOPOLOGY_ID_0 = "subtopology-0";
    private static final String SUB_TOPOLOGY_ID_1 = "subtopology-1";

    private static final String TOPIC_0 = "topic-0";
    private static final String TOPIC_1 = "topic-1";

    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;

    private Time time = new MockTime(0);
    private Metrics metrics = new Metrics(time);

    private StreamsMembershipManager membershipManager;

    @Mock
    private SubscriptionState subscriptionState;

    @Mock
    private StreamsAssignmentInterface streamsAssignmentInterface;

    @Captor
    private ArgumentCaptor<StreamsOnTasksAssignedCallbackNeededEvent> onAssignmentCallbackNeededEventCaptor;

    private Queue<BackgroundEvent> backgroundEventQueue = new LinkedList<>();
    private BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue);

    @BeforeEach
    public void setup() {
        membershipManager = new StreamsMembershipManager(
            GROUP_ID,
            streamsAssignmentInterface,
            subscriptionState,
            new LogContext("test"),
            time,
            metrics
        );
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testUnexpectedErrorInHeartbeatResponse() {
        final String errorMessage = "Nobody expects the Spanish Inquisition!";
        final StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())
            .setErrorMessage(errorMessage);
        final StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(responseData);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> membershipManager.onHeartbeatSuccess(response)
        );

        assertEquals(
            "Unexpected error in Heartbeat response. Expected no error, but received: "
                + Errors.GROUP_AUTHORIZATION_FAILED.name()
                + " with message: '" + errorMessage + "'",
            exception.getMessage()
        );
    }

    @Test
    public void testActiveTasksAreNullInHeartbeatResponse() {
        joining();
        final StreamsGroupHeartbeatResponse response = makeHeartbeatResponse(null);

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> membershipManager.onHeartbeatSuccess(response)
        );

        assertEquals(
            "Invalid response data, task collections must be all null or all non-null: " + response.data(),
            exception.getMessage()
        );
    }

    @Test
    public void testJoining() {
        joining();

        verifyInStateJoining(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testReconcilingEmptyToSingleActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Collection<TopicPartition> expectedPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedPartitionsToAssign, expectedPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
        onTasksAssignedCallbackExecuted.complete(null);
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
        verify(streamsAssignmentInterface, never()).requestOnTasksRevokedCallbackInvocation(any());
    }

    @Test
    public void testReconcilingActiveTaskToDifferentActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        verify(subscriptionState).markPendingRevocation(expectedPartitionsToRevoke);
        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedPartitionsToAssign, expectedPartitionsToAssign);
        onTasksRevokedCallbackExecuted.complete(null);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedPartitionsToAssign, expectedPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
        onTasksAssignedCallbackExecuted.complete(null);
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
    }

    @Test
    public void testReconcilingSingleActiveTaskToAdditionalActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        final Collection<TopicPartition> expectedPartitionAssignment = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_0, PARTITION_1)
        );
        final Collection<TopicPartition> expectedAdditionalPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedPartitionAssignment, expectedAdditionalPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedPartitionAssignment);
        verifyInStateReconciling(membershipManager);
        onTasksAssignedCallbackExecuted.complete(null);
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedAdditionalPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
        verify(streamsAssignmentInterface, never()).requestOnTasksRevokedCallbackInvocation(any());
    }

    @Test
    public void testReconcilingMultipleActiveTaskToSingleActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasksToRevoke = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksToRevoke))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0), new TopicPartition(TOPIC_0, PARTITION_1)));
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Collection<TopicPartition> expectedAdditionalPartitionsToAssign = Collections.emptySet();
        verify(subscriptionState).markPendingRevocation(expectedPartitionsToRevoke);
        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedPartitionsToAssign, expectedAdditionalPartitionsToAssign);
        onTasksRevokedCallbackExecuted.complete(null);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedPartitionsToAssign, expectedAdditionalPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedAdditionalPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
        onTasksAssignedCallbackExecuted.complete(null);
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedAdditionalPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
    }

    @Test
    public void testReconcilingEmptyToMultipleActiveTaskOfDifferentSubtopologies() {
        setupStreamsAssignmentInterfaceWithTwoSubtopologies(
            SUB_TOPOLOGY_ID_0, TOPIC_0,
            SUB_TOPOLOGY_ID_1, TOPIC_1
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_1, PARTITION_0)
        );
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponse(
            SUB_TOPOLOGY_ID_0, List.of(PARTITION_0),
            SUB_TOPOLOGY_ID_1, List.of(PARTITION_0))
        );

        final Collection<TopicPartition> expectedPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_1, PARTITION_0)
        );
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedPartitionsToAssign, expectedPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
        onTasksAssignedCallbackExecuted.complete(null);
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
        verify(streamsAssignmentInterface, never()).requestOnTasksRevokedCallbackInvocation(any());
    }

    @Test
    public void testReconcilingAndAssignmentCallbackFails() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Collection<TopicPartition> expectedPartitionAssignment = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedPartitionAssignment, expectedPartitionAssignment);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedPartitionAssignment);
        verifyInStateReconciling(membershipManager);
        onTasksAssignedCallbackExecuted.completeExceptionally(new RuntimeException("KABOOM!"));
        verifyInStateReconciling(membershipManager);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedPartitionAssignment);
    }

    @Test
    public void testLeaveGroupWhenNotInGroup() {
        final CompletableFuture<Void> future = membershipManager.leaveGroup();

        assertFalse(membershipManager.isLeavingGroup());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        verify(subscriptionState).unsubscribe();
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenNotInGroupAndFenced() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        fenced();
        final CompletableFuture<Void> future = membershipManager.leaveGroup();

        assertFalse(membershipManager.isLeavingGroup());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        verify(subscriptionState).unsubscribe();
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenInGroupWithAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasks))
            .thenReturn(onTasksRevokedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroup();

        assertFalse(onGroupLeft.isDone());
        verify(subscriptionState, never()).unsubscribe();
        verifyInStatePrepareLeaving(membershipManager);
        final CompletableFuture<Void> onGroupLeftBeforeRevocationCallback = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftBeforeRevocationCallback);
        onTasksRevokedCallbackExecuted.complete(null);
        verify(subscriptionState).unsubscribe();
        assertFalse(onGroupLeft.isDone());
        verifyInStateLeaving(membershipManager);
        final CompletableFuture<Void> onGroupLeftAfterRevocationCallback = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftAfterRevocationCallback);
        membershipManager.transitionToUnsubscribeIfLeaving();
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testTransitionToUnsubscribeWhenInLeaving() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, "topic");
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksRevokedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onAllTasksRevokedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        CompletableFuture<Void> future = leaving(onAllTasksRevokedCallbackExecuted);

        membershipManager.transitionToUnsubscribeIfLeaving();

        verifyInStateUnsubscribed(membershipManager);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnPollTimerExpired() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onPollTimerExpired();

        verifyInStateLeaving(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInAcknowleding() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInAcknowledgingAndNewTargetAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateReconciling(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        leaving(onAllTasksLostCallbackExecuted);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInLeavingAndPollTimerExpired() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        membershipManager.onPollTimerExpired();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStale(membershipManager);
    }

    @Test
    public void testOnFencedWhenInJoining() {
        joining();

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInAcknowledging() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInStable() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        stable();

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    private void testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);

        membershipManager.onFenced();

        verifyInStateFenced(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
        onAllTasksLostCallbackExecuted.complete(null);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verifyInStateJoining(membershipManager);
    }

    @Test
    public void testOnFencedWhenInPrepareLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testOnFencedWhenInPrepareLeavingOrLeaving(prepareLeaving());
    }

    @Test
    public void testOnFencedWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testOnFencedWhenInPrepareLeavingOrLeaving(leaving(onAllTasksLostCallbackExecuted));
    }

    private void testOnFencedWhenInPrepareLeavingOrLeaving(final CompletableFuture<Void> onGroupLeft) {
        membershipManager.onFenced();

        verifyInStateUnsubscribed(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCancelled());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testTransitionToFatalWhenInPrepareLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInPrepareLeavingOrLeaving(prepareLeaving());
    }

    @Test
    public void testTransitionToFatalWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInPrepareLeavingOrLeaving(leaving(onAllTasksLostCallbackExecuted));
    }

    private void testTransitionToFatalWhenInPrepareLeavingOrLeaving(final CompletableFuture<Void> onGroupLeft) {
        membershipManager.transitionToFatal();

        verifyInStateFatal(membershipManager);
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCancelled());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testTransitionToFatalWhenInJoining() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInAcknowledging() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInStable() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup)))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponse(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        stable();

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    private void testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(final CompletableFuture<Void> future) {
        membershipManager.transitionToFatal();

        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
        future.complete(null);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verifyInStateFatal(membershipManager);
    }

    @Test
    public void testOnTasksAssignedCallbackCompleted() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnTasksAssignedCallbackCompletedEvent event = new StreamsOnTasksAssignedCallbackCompletedEvent(
            future,
            Optional.empty()
        );

        membershipManager.onTasksAssignedCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnAllTasksLostCallbackCompleted() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnAllTasksLostCallbackCompletedEvent event = new StreamsOnAllTasksLostCallbackCompletedEvent(
            future,
            Optional.empty()
        );

        membershipManager.onAllTasksLostCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnTasksAssignedCallbackCompletedWhenCallbackFails() {
        final String errorMessage = "KABOOM!";
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnAllTasksLostCallbackCompletedEvent event = new StreamsOnAllTasksLostCallbackCompletedEvent(
            future,
            Optional.of(new KafkaException(errorMessage))
        );

        membershipManager.onAllTasksLostCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        final ExecutionException executionException = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(KafkaException.class, executionException.getCause());
        assertEquals(errorMessage, executionException.getCause().getMessage());
    }

    private static void verifyInStateReconciling(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateAcknowledging(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStatePrepareLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateUnsubscribed(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateJoining(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateStable(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateFenced(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.FENCED, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateFatal(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.FATAL, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateStale(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.STALE, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private void setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(final String subtopologyId,
                                                                                 final String topicName) {
        when(streamsAssignmentInterface.subtopologyMap()).thenReturn(
            mkMap(
                mkEntry(
                    subtopologyId,
                    new StreamsAssignmentInterface.Subtopology(
                        Set.of(topicName),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                )
            )
        );
    }

    private void setupStreamsAssignmentInterfaceWithTwoSubtopologies(final String subtopologyId1,
                                                                     final String topicName1,
                                                                     final String subtopologyId2,
                                                                     final String topicName2) {
        when(streamsAssignmentInterface.subtopologyMap()).thenReturn(
            mkMap(
                mkEntry(
                    subtopologyId1,
                    new StreamsAssignmentInterface.Subtopology(
                        Set.of(topicName1),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                ),
                mkEntry(
                    subtopologyId2,
                    new StreamsAssignmentInterface.Subtopology(
                        Set.of(topicName2),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                )
            )
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponse(final String subtopologyId,
                                                                final List<Integer> partitions) {
        return makeHeartbeatResponse(Collections.singletonList(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(partitions)
        ));
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponse(final String subtopologyId0,
                                                                final List<Integer> partitions0,
                                                                final String subtopologyId1,
                                                                final List<Integer> partitions1) {
        return makeHeartbeatResponse(List.of(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId0)
                .setPartitions(partitions0),
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId1)
                .setPartitions(partitions1)
        ));
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponse(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks) {
        final StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setActiveTasks(activeTasks)
            .setStandbyTasks(Collections.emptyList())
            .setWarmupTasks(Collections.emptyList());
        return new StreamsGroupHeartbeatResponse(responseData);
    }

    private StreamsAssignmentInterface.Assignment makeTaskAssignment(final Set<StreamsAssignmentInterface.TaskId> activeTasks) {
        return new StreamsAssignmentInterface.Assignment(
            activeTasks,
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private void joining() {
        membershipManager.onSubscriptionUpdated();
        membershipManager.onConsumerPoll();
        verifyInStateJoining(membershipManager);
    }

    private void reconcile(final StreamsGroupHeartbeatResponse response) {
        membershipManager.onHeartbeatSuccess(response);
        membershipManager.poll(time.milliseconds());
        verifyInStateReconciling(membershipManager);

    }

    private void acknowledging() {
        final StreamsOnTasksAssignedCallbackNeededEvent onAssignmentCallbackNeededEvent =
            (StreamsOnTasksAssignedCallbackNeededEvent) backgroundEventQueue.poll();
        onAssignmentCallbackNeededEvent.future().complete(null);
        verifyInStateAcknowledging(membershipManager);
    }

    private void acknowledging(final CompletableFuture<Void> future) {
        future.complete(null);
        verifyInStateAcknowledging(membershipManager);
    }

    private CompletableFuture<Void> prepareLeaving() {
        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroup();
        verifyInStatePrepareLeaving(membershipManager);
        return onGroupLeft;
    }

    private CompletableFuture<Void> leaving(final CompletableFuture<Void> onAllTasksRevokedCallbackExecuted) {
        final CompletableFuture<Void> future = prepareLeaving();
        onAllTasksRevokedCallbackExecuted.complete(null);
        verifyInStateLeaving(membershipManager);
        return future;
    }

    private void stable() {
        membershipManager.onHeartbeatRequestGenerated();
    }

    private void fenced() {
        membershipManager.onFenced();
    }
}
