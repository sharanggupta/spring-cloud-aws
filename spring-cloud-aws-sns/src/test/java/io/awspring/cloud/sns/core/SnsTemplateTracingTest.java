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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.awspring.cloud.sns.observation.W3cTracing;
import io.micrometer.observation.Observation;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * Tests for the propagation of the trace context with the messages published by {@link SnsTemplate}.
 *
 * @author Sharang Gupta
 */
class SnsTemplateTracingTest {

	private static final String TOPIC_ARN = "arn:aws:sns:eu-west-1:123456789012:orders";

	private final SnsClient snsClient = mock(SnsClient.class);

	private final W3cTracing tracing = new W3cTracing();

	private SnsTemplate snsTemplate;

	@BeforeEach
	void setUp() {
		given(snsClient.publish(any(PublishRequest.class)))
				.willReturn(PublishResponse.builder().messageId("published-message-id").build());
		snsTemplate = new SnsTemplate(snsClient, topicName -> Arn.fromString(TOPIC_ARN), null);
		snsTemplate.setObservationRegistry(tracing.observationRegistry());
	}

	@Test
	void sendsTheTraceContextOfTheCurrentObservationAsAMessageAttribute() {
		Observation parent = Observation.start("parent", tracing.observationRegistry());
		String expectedTraceId;
		try (Observation.Scope ignored = parent.openScope()) {
			expectedTraceId = tracing.currentTraceId();
			snsTemplate.convertAndSend("orders", "order-created");
		}
		finally {
			parent.stop();
		}

		MessageAttributeValue traceparent = publishedRequest().messageAttributes().get("traceparent");
		assertThat(traceparent).isNotNull();
		assertThat(traceparent.dataType()).isEqualTo("String");
		assertThat(traceparent.stringValue()).contains(expectedTraceId);
	}

	@Test
	void doesNotReplaceATraceContextProvidedByTheCaller() {
		String callerTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

		snsTemplate.convertAndSend("orders", "order-created", Map.of("traceparent", callerTraceparent));

		assertThat(publishedRequest().messageAttributes().get("traceparent").stringValue())
				.isEqualTo(callerTraceparent);
	}

	@Test
	void doesNotAddTheTraceContextWhenSnsWouldRejectTheMessageForHavingTooManyAttributes() {
		Map<String, Object> headers = new HashMap<>();
		// the id and the timestamp of the message are sent as attributes too, which makes 10, the limit of SNS
		for (int i = 0; i < 8; i++) {
			headers.put("header-" + i, "value-" + i);
		}

		snsTemplate.convertAndSend("orders", "order-created", headers);

		assertThat(publishedRequest().messageAttributes()).hasSize(10).doesNotContainKey("traceparent");
		TestObservationRegistryAssert.then(tracing.observationRegistry())
				.hasObservationWithNameEqualTo("spring.aws.sns.template");
	}

	private PublishRequest publishedRequest() {
		ArgumentCaptor<PublishRequest> request = ArgumentCaptor.forClass(PublishRequest.class);
		verify(snsClient).publish(request.capture());
		return request.getValue();
	}

}
