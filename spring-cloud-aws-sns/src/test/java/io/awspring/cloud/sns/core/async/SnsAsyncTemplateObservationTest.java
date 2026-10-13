/*
 * Copyright 2013-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sns.core.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.awspring.cloud.sns.core.SnsHeaders;
import io.awspring.cloud.sns.observation.W3cTracing;
import io.micrometer.observation.Observation;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

/**
 * Tests for the observation of the messages published by {@link SnsAsyncTemplate}.
 *
 * @author Sharang Gupta
 */
class SnsAsyncTemplateObservationTest {

	private static final String TOPIC_ARN = "arn:aws:sns:eu-west-1:123456789012:orders";

	private final SnsAsyncClient snsAsyncClient = mock(SnsAsyncClient.class);

	private final W3cTracing tracing = new W3cTracing();

	private final TestObservationRegistry observationRegistry = tracing.observationRegistry();

	private SnsAsyncTemplate snsAsyncTemplate;

	@BeforeEach
	void setUp() {
		given(snsAsyncClient.publish(any(PublishRequest.class))).willReturn(
				CompletableFuture.completedFuture(PublishResponse.builder().messageId("published-message-id").build()));
		snsAsyncTemplate = new SnsAsyncTemplate(snsAsyncClient, topicName -> Arn.fromString(TOPIC_ARN), null);
		snsAsyncTemplate.setObservationRegistry(observationRegistry);
	}

	@Test
	void observesThePublishedMessage() {
		snsAsyncTemplate.convertAndSend("orders", "order-created").join();

		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().hasNameEqualTo("spring.aws.sns.template")
				.hasContextualNameEqualTo("orders send").hasLowCardinalityKeyValue("messaging.system", "sns")
				.hasLowCardinalityKeyValue("messaging.operation", "publish")
				.hasLowCardinalityKeyValue("messaging.destination.name", "orders")
				.hasLowCardinalityKeyValue("messaging.destination.kind", "topic")
				.hasHighCardinalityKeyValue("messaging.message.id", "published-message-id").hasBeenStopped();
	}

	@Test
	void keepsTheObservationOpenUntilThePublishCompletes() {
		CompletableFuture<PublishResponse> pendingPublish = new CompletableFuture<>();
		given(snsAsyncClient.publish(any(PublishRequest.class))).willReturn(pendingPublish);

		CompletableFuture<?> result = snsAsyncTemplate.convertAndSend("orders", "order-created");

		assertThat(result).isNotDone();
		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().hasBeenStarted().isNotStopped();

		pendingPublish.complete(PublishResponse.builder().messageId("late-message-id").build());
		result.join();

		TestObservationRegistryAssert.then(observationRegistry).hasSingleObservationThat().hasBeenStopped()
				.hasHighCardinalityKeyValue("messaging.message.id", "late-message-id");
	}

	@Test
	void addsTheFifoIdsToTheObservation() {
		snsAsyncTemplate.convertAndSend("orders", "order-created", Map.of(SnsHeaders.MESSAGE_GROUP_ID_HEADER, "group-1",
				SnsHeaders.MESSAGE_DEDUPLICATION_ID_HEADER, "deduplication-1")).join();

		TestObservationRegistryAssert.then(observationRegistry).hasSingleObservationThat()
				.hasHighCardinalityKeyValue("messaging.message.message-group.id", "group-1")
				.hasHighCardinalityKeyValue("messaging.message.message-deduplication.id", "deduplication-1");
	}

	@Test
	void recordsTheErrorWhenThePublishFails() {
		SnsException failure = (SnsException) SnsException.builder().message("publish failed").build();
		given(snsAsyncClient.publish(any(PublishRequest.class))).willReturn(CompletableFuture.failedFuture(failure));

		assertThatThrownBy(() -> snsAsyncTemplate.convertAndSend("orders", "order-created").join());

		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().assertThatError().isSameAs(failure);
		TestObservationRegistryAssert.then(observationRegistry).hasSingleObservationThat().hasBeenStopped();
	}

	@Test
	void sendsTheTraceContextOfTheObservationThatWasCurrentWhenSendingAsAMessageAttribute() {
		Observation parent = Observation.start("parent", observationRegistry);
		String expectedTraceId;
		CompletableFuture<?> result;
		try (Observation.Scope ignored = parent.openScope()) {
			expectedTraceId = tracing.currentTraceId();
			result = snsAsyncTemplate.convertAndSend("orders", "order-created");
		}
		finally {
			parent.stop();
		}
		result.join();

		ArgumentCaptor<PublishRequest> request = ArgumentCaptor.forClass(PublishRequest.class);
		verify(snsAsyncClient).publish(request.capture());
		assertThat(request.getValue().messageAttributes().get("traceparent").stringValue()).contains(expectedTraceId);
	}

	@Test
	void doesNotObserveWithoutAnObservationRegistry() {
		SnsAsyncTemplate templateWithoutRegistry = new SnsAsyncTemplate(snsAsyncClient,
				topicName -> Arn.fromString(TOPIC_ARN), null);

		templateWithoutRegistry.convertAndSend("orders", "order-created").join();

		ArgumentCaptor<PublishRequest> request = ArgumentCaptor.forClass(PublishRequest.class);
		verify(snsAsyncClient).publish(request.capture());
		assertThat(request.getValue().messageAttributes()).doesNotContainKey("traceparent");
		TestObservationRegistryAssert.then(observationRegistry).doesNotHaveAnyObservation();
	}

}
