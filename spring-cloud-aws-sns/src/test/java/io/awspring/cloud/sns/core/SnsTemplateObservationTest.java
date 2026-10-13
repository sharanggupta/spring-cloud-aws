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
package io.awspring.cloud.sns.core;

import static io.awspring.cloud.sns.Matchers.requestMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.awspring.cloud.sns.core.observation.SnsTemplateObservation;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.MessageHeaders;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

/**
 * Tests for the observation of the messages published by {@link SnsTemplate}.
 *
 * @author Sharang Gupta
 */
class SnsTemplateObservationTest {

	private static final String TOPIC_ARN = "arn:aws:sns:eu-west-1:123456789012:orders";

	private final SnsClient snsClient = mock(SnsClient.class);

	private final TestObservationRegistry observationRegistry = TestObservationRegistry.create();

	private SnsTemplate snsTemplate;

	@BeforeEach
	void setUp() {
		given(snsClient.publish(any(PublishRequest.class)))
				.willReturn(PublishResponse.builder().messageId("published-message-id").build());
		snsTemplate = new SnsTemplate(snsClient, topicName -> Arn.fromString(TOPIC_ARN), null);
		snsTemplate.setObservationRegistry(observationRegistry);
	}

	@Test
	void observesThePublishedMessage() {
		snsTemplate.convertAndSend("orders", "order-created");

		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().hasNameEqualTo("spring.aws.sns.template")
				.hasContextualNameEqualTo("orders send").hasLowCardinalityKeyValue("messaging.system", "sns")
				.hasLowCardinalityKeyValue("messaging.operation", "publish")
				.hasLowCardinalityKeyValue("messaging.destination.name", "orders")
				.hasLowCardinalityKeyValue("messaging.destination.kind", "topic").hasBeenStopped();
	}

	@Test
	void addsTheIdOfThePublishedMessageToTheObservation() {
		snsTemplate.convertAndSend("orders", "order-created");

		TestObservationRegistryAssert.then(observationRegistry).hasSingleObservationThat()
				.hasHighCardinalityKeyValue("messaging.message.id", "published-message-id");
	}

	@Test
	void addsTheFifoIdsToTheObservation() {
		snsTemplate.convertAndSend("orders", "order-created", Map.of(SnsHeaders.MESSAGE_GROUP_ID_HEADER, "group-1",
				SnsHeaders.MESSAGE_DEDUPLICATION_ID_HEADER, "deduplication-1"));

		TestObservationRegistryAssert.then(observationRegistry).hasSingleObservationThat()
				.hasHighCardinalityKeyValue("messaging.message.message-group.id", "group-1")
				.hasHighCardinalityKeyValue("messaging.message.message-deduplication.id", "deduplication-1");
	}

	@Test
	void observesMessagesSentWithSend() {
		snsTemplate.send("orders",
				org.springframework.messaging.support.MessageBuilder.withPayload("order-created").build());

		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().hasLowCardinalityKeyValue("messaging.destination.name", "orders");
	}

	@Test
	void observesMessagesSentToTheDefaultDestinationConfiguredBeforeTheObservationRegistry() {
		SnsTemplate template = new SnsTemplate(snsClient, topicName -> Arn.fromString(TOPIC_ARN), null);
		template.setDefaultDestinationName("orders");
		template.setObservationRegistry(observationRegistry);

		template.convertAndSend("order-created");

		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().hasLowCardinalityKeyValue("messaging.destination.name", "orders");
	}

	@Test
	void recordsTheErrorWhenThePublishFails() {
		SnsException failure = (SnsException) SnsException.builder().message("publish failed").build();
		given(snsClient.publish(any(PublishRequest.class))).willThrow(failure);

		assertThatThrownBy(() -> snsTemplate.convertAndSend("orders", "order-created"));

		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().assertThatError().isSameAs(failure);
		TestObservationRegistryAssert.then(observationRegistry).hasSingleObservationThat().hasBeenStopped();
	}

	@Test
	void supportsACustomObservationConvention() {
		snsTemplate.setObservationConvention(new SnsTemplateObservation.DefaultConvention() {
			@Override
			public String getName() {
				return "custom.sns.publish";
			}
		});

		snsTemplate.convertAndSend("orders", "order-created");

		TestObservationRegistryAssert.then(observationRegistry).hasNumberOfObservationsEqualTo(1)
				.hasSingleObservationThat().hasNameEqualTo("custom.sns.publish");
	}

	@Test
	void doesNotObserveWithoutAnObservationRegistry() {
		SnsTemplate templateWithoutRegistry = new SnsTemplate(snsClient, topicName -> Arn.fromString(TOPIC_ARN), null);

		templateWithoutRegistry.convertAndSend("orders", "order-created");

		verify(snsClient).publish(requestMatches(request -> assertThat(request.messageAttributes().keySet())
				.containsExactlyInAnyOrder(MessageHeaders.ID, MessageHeaders.TIMESTAMP)));
		TestObservationRegistryAssert.then(observationRegistry).doesNotHaveAnyObservation();
	}

}
