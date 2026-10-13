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
import static org.awaitility.Awaitility.await;

import io.awspring.cloud.sns.core.async.SnsAsyncTemplate;
import io.awspring.cloud.sns.observation.W3cTracing;
import io.micrometer.observation.Observation;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Integration tests that verify that the trace context of the observed messages reaches the subscribers of the topic.
 *
 * @author Sharang Gupta
 */
@Testcontainers
class SnsTemplateObservationIntegrationTest {

	private static final JsonMapper jsonMapper = JsonMapper.builder().build();

	@Container
	static LocalStackContainer localstack = new LocalStackContainer(
			DockerImageName.parse("localstack/localstack:4.4.0"));

	private static SnsClient snsClient;

	private static SnsAsyncClient snsAsyncClient;

	private static SqsClient sqsClient;

	private static String queueUrl;

	private static String topicArn;

	private final W3cTracing tracing = new W3cTracing();

	@BeforeAll
	static void setUp() {
		StaticCredentialsProvider credentials = StaticCredentialsProvider
				.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()));
		snsClient = SnsClient.builder().endpointOverride(localstack.getEndpoint())
				.region(Region.of(localstack.getRegion())).credentialsProvider(credentials).build();
		snsAsyncClient = SnsAsyncClient.builder().endpointOverride(localstack.getEndpoint())
				.region(Region.of(localstack.getRegion())).credentialsProvider(credentials).build();
		sqsClient = SqsClient.builder().endpointOverride(localstack.getEndpoint())
				.region(Region.of(localstack.getRegion())).credentialsProvider(credentials).build();

		queueUrl = sqsClient.createQueue(request -> request.queueName("observation-queue")).queueUrl();
		String queueArn = sqsClient
				.getQueueAttributes(request -> request.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
				.attributes().get(QueueAttributeName.QUEUE_ARN);
		topicArn = snsClient.createTopic(request -> request.name("observation-topic")).topicArn();
		snsClient.subscribe(request -> request.topicArn(topicArn).protocol("sqs").endpoint(queueArn));
	}

	@AfterEach
	void purgeQueue() {
		sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
	}

	@Test
	void snsTemplateSendsTheTraceContextToTheSubscribers() throws Exception {
		SnsTemplate snsTemplate = new SnsTemplate(snsClient);
		snsTemplate.setObservationRegistry(tracing.observationRegistry());

		String traceId = sendWithinAParentObservation(() -> snsTemplate.convertAndSend(topicArn, "order-created"));

		assertThat(traceparentOf(receiveEnvelope())).contains(traceId);
		TestObservationRegistryAssert.then(tracing.observationRegistry())
				.hasObservationWithNameEqualTo("spring.aws.sns.template");
	}

	@Test
	void snsAsyncTemplateSendsTheTraceContextToTheSubscribers() throws Exception {
		SnsAsyncTemplate snsAsyncTemplate = new SnsAsyncTemplate(snsAsyncClient);
		snsAsyncTemplate.setObservationRegistry(tracing.observationRegistry());

		String traceId = sendWithinAParentObservation(
				() -> snsAsyncTemplate.convertAndSend(topicArn, "order-created").join());

		assertThat(traceparentOf(receiveEnvelope())).contains(traceId);
		TestObservationRegistryAssert.then(tracing.observationRegistry())
				.hasObservationWithNameEqualTo("spring.aws.sns.template");
	}

	private String sendWithinAParentObservation(Runnable send) {
		Observation parent = Observation.start("parent", tracing.observationRegistry());
		try (Observation.Scope ignored = parent.openScope()) {
			String traceId = tracing.currentTraceId();
			send.run();
			return traceId;
		}
		finally {
			parent.stop();
		}
	}

	private static String traceparentOf(JsonNode envelope) {
		JsonNode traceparent = envelope.get("MessageAttributes").get("traceparent");
		assertThat(traceparent).as("the traceparent message attribute").isNotNull();
		return traceparent.get("Value").asString();
	}

	private static JsonNode receiveEnvelope() throws Exception {
		List<Message> messages = new ArrayList<>();
		await().atMost(Duration.ofSeconds(15)).until(() -> {
			messages.addAll(sqsClient.receiveMessage(request -> request.queueUrl(queueUrl).waitTimeSeconds(2)
					.maxNumberOfMessages(1).messageAttributeNames("All")).messages());
			return !messages.isEmpty();
		});
		return jsonMapper.readTree(messages.get(0).body());
	}

}
