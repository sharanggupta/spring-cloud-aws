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

import static io.awspring.cloud.sns.core.SnsHeaders.NOTIFICATION_SUBJECT_HEADER;

import io.awspring.cloud.sns.core.CachingTopicArnResolver;
import io.awspring.cloud.sns.core.SnsAsyncTopicArnResolver;
import io.awspring.cloud.sns.core.SnsNotification;
import io.awspring.cloud.sns.core.TopicArnResolver;
import io.awspring.cloud.sns.core.observation.SnsPublishObserver;
import io.awspring.cloud.sns.core.observation.SnsTemplateObservation;
import io.micrometer.observation.ObservationRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.jspecify.annotations.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.core.MessagePostProcessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.NotFoundException;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * Asynchronous template for SNS operations.
 *
 * @author Matej Nedic
 * @author Sharang Gupta
 * @since 4.1.0
 */
public class SnsAsyncTemplate implements SnsAsyncOperations {

	private final SnsAsyncClient snsAsyncClient;
	private final TopicArnResolver topicArnResolver;
	private final SnsPublishMessageConverter snsPublishMessageConverter;
	private ObservationRegistry observationRegistry = ObservationRegistry.NOOP;
	private SnsTemplateObservation.@Nullable Convention observationConvention;
	private volatile @Nullable SnsPublishObserver publishObserver;

	public SnsAsyncTemplate(SnsAsyncClient snsAsyncClient) {
		this(snsAsyncClient, null);
	}

	public SnsAsyncTemplate(SnsAsyncClient snsAsyncClient, @Nullable SnsPublishMessageConverter messageConverter) {
		this(snsAsyncClient, new CachingTopicArnResolver(new SnsAsyncTopicArnResolver(snsAsyncClient)),
				messageConverter);
	}

	public SnsAsyncTemplate(SnsAsyncClient snsAsyncClient, TopicArnResolver topicArnResolver,
			@Nullable SnsPublishMessageConverter messageConverter) {
		Assert.notNull(snsAsyncClient, "SnsAsyncClient must not be null");
		Assert.notNull(topicArnResolver, "TopicArnResolver must not be null");
		this.topicArnResolver = topicArnResolver;
		this.snsAsyncClient = snsAsyncClient;
		this.snsPublishMessageConverter = messageConverter == null ? new DefaultSnsPublishMessageConverter()
				: messageConverter;
	}

	/**
	 * Set the {@link ObservationRegistry} to record the observations of the published messages with. By default, the
	 * messages are not observed. The trace context of the observations is sent as message attributes.
	 *
	 * @param observationRegistry the registry.
	 * @since 4.3.0
	 */
	public void setObservationRegistry(ObservationRegistry observationRegistry) {
		Assert.notNull(observationRegistry, "observationRegistry must not be null");
		this.observationRegistry = observationRegistry;
		this.publishObserver = SnsPublishObserver.create(this.observationRegistry, this.observationConvention);
	}

	/**
	 * Set the {@link SnsTemplateObservation.Convention} to use instead of the default one for the observations of the
	 * published messages.
	 *
	 * @param observationConvention the convention.
	 * @since 4.3.0
	 */
	public void setObservationConvention(SnsTemplateObservation.Convention observationConvention) {
		Assert.notNull(observationConvention, "observationConvention must not be null");
		this.observationConvention = observationConvention;
		this.publishObserver = SnsPublishObserver.create(this.observationRegistry, this.observationConvention);
	}

	@Override
	public <T> CompletableFuture<PublishMessageResult<T>> send(String destination, Message<T> message) {
		Assert.notNull(destination, "destination cannot be null");
		Assert.notNull(message, "message cannot be null");

		Arn topicArn = topicArnResolver.resolveTopicArn(destination);

		PublishRequestMessagePair<T> pair = snsPublishMessageConverter.convert(message);
		PublishRequest request = pair.publishRequest().toBuilder().topicArn(topicArn.toString()).build();

		return publish(request, pair.originalMessage(), topicArn.resourceAsString());
	}

	@Override
	public <T> CompletableFuture<PublishMessageResult<T>> convertAndSend(String destination, T payload) {
		Assert.notNull(destination, "destination cannot be null");
		Assert.notNull(payload, "payload cannot be null");

		return convertAndSend(destination, payload, null, null);
	}

	@Override
	public <T> CompletableFuture<PublishMessageResult<T>> convertAndSend(String destination, T payload,
			@Nullable Map<String, Object> headers) {
		Assert.notNull(destination, "destination cannot be null");
		Assert.notNull(payload, "payload cannot be null");

		return convertAndSend(destination, payload, headers, null);
	}

	@Override
	public <T> CompletableFuture<PublishMessageResult<T>> convertAndSend(String destination, T payload,
			@Nullable MessagePostProcessor postProcessor) {
		Assert.notNull(destination, "destination cannot be null");
		Assert.notNull(payload, "payload cannot be null");

		return convertAndSend(destination, payload, null, postProcessor);
	}

	@Override
	public <T> CompletableFuture<PublishMessageResult<T>> convertAndSend(String destination, T payload,
			@Nullable Map<String, Object> headers, @Nullable MessagePostProcessor postProcessor) {
		Assert.notNull(destination, "destination cannot be null");
		Assert.notNull(payload, "payload cannot be null");

		Message<T> message = MessageBuilder.withPayload(payload)
				.copyHeaders(headers != null ? headers : Collections.emptyMap()).build();

		if (postProcessor != null) {
			message = (Message<T>) postProcessor.postProcessMessage(message);
		}

		return send(destination, message);
	}

	@Override
	public CompletableFuture<PublishMessageResult<Object>> sendNotification(String destinationName, Object message,
			@Nullable String subject) {
		Assert.notNull(destinationName, "destinationName cannot be null");
		Assert.notNull(message, "message cannot be null");

		return convertAndSend(destinationName, message, Collections.singletonMap(NOTIFICATION_SUBJECT_HEADER, subject));
	}

	@Override
	public <T> CompletableFuture<PublishMessageResult<T>> sendNotification(String topic,
			SnsNotification<T> notification) {
		Assert.notNull(topic, "topic cannot be null");
		Assert.notNull(notification, "notification cannot be null");

		return convertAndSend(topic, notification.getPayload(), notification.getHeaders());
	}

	@Override
	public CompletableFuture<Boolean> topicExists(String topicArn) {
		Assert.notNull(topicArn, "topicArn must not be null");

		return snsAsyncClient.getTopicAttributes(request -> request.topicArn(topicArn)).thenApply(response -> true)
				.exceptionally(throwable -> {
					if (throwable.getCause() instanceof NotFoundException) {
						return false;
					}
					if (throwable.getCause() instanceof RuntimeException re) {
						throw re;
					}
					throw new RuntimeException("Unexpected exception", throwable);
				});
	}

	private <T> CompletableFuture<PublishMessageResult<T>> publish(PublishRequest request, Message<T> originalMessage,
			String topicName) {
		SnsPublishObserver observer = this.publishObserver;
		if (observer == null) {
			return doPublish(request, originalMessage);
		}
		SnsPublishObserver.Publication publication = observer.start(originalMessage, topicName);
		CompletableFuture<PublishMessageResult<T>> result;
		try {
			result = doPublish(publication.withTraceContext(request), originalMessage);
		}
		catch (RuntimeException ex) {
			publication.failed(ex);
			publication.stop();
			throw ex;
		}
		return result.whenComplete((publishResult, throwable) -> {
			if (throwable != null) {
				publication.failed(unwrap(throwable));
			}
			else {
				publication.published(publishResult.messageId());
			}
			publication.stop();
		});
	}

	private <T> CompletableFuture<PublishMessageResult<T>> doPublish(PublishRequest request,
			Message<T> originalMessage) {
		return snsAsyncClient.publish(request).thenApply(response -> new PublishMessageResult<>(originalMessage,
				response.messageId(), response.sequenceNumber()));
	}

	private static Throwable unwrap(Throwable throwable) {
		return throwable instanceof CompletionException && throwable.getCause() != null ? throwable.getCause()
				: throwable;
	}
}
