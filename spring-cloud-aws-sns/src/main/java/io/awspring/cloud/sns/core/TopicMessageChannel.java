/*
 * Copyright 2013-2022 the original author or authors.
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

import static io.awspring.cloud.sns.core.SnsHeaders.MESSAGE_DEDUPLICATION_ID_HEADER;
import static io.awspring.cloud.sns.core.SnsHeaders.MESSAGE_GROUP_ID_HEADER;
import static io.awspring.cloud.sns.core.SnsHeaders.NOTIFICATION_SUBJECT_HEADER;

import io.awspring.cloud.sns.core.observation.SnsPublishObserver;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.AbstractMessageChannel;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * Implementation of {@link AbstractMessageChannel} which is used for converting and sending messages via
 * {@link SnsClient} to SNS.
 *
 * @author Agim Emruli
 * @author Alain Sahli
 * @author Gyozo Papp
 * @author Sharang Gupta
 * @since 1.0
 */
public class TopicMessageChannel extends AbstractMessageChannel {

	private final SnsClient snsClient;

	private final Arn topicArn;

	private volatile @Nullable SnsPublishObserver publishObserver;

	public TopicMessageChannel(SnsClient snsClient, Arn topicArn) {
		this(snsClient, topicArn, null);
	}

	/**
	 * Create a channel that observes the messages it publishes.
	 *
	 * @param snsClient the client to publish with.
	 * @param topicArn the ARN of the topic to publish to.
	 * @param publishObserver the observer of the published messages, or {@code null} to not observe them.
	 * @since 4.3.0
	 */
	public TopicMessageChannel(SnsClient snsClient, Arn topicArn, @Nullable SnsPublishObserver publishObserver) {
		this.snsClient = snsClient;
		this.topicArn = topicArn;
		this.publishObserver = publishObserver;
	}

	void setPublishObserver(@Nullable SnsPublishObserver publishObserver) {
		this.publishObserver = publishObserver;
	}

	@Nullable
	private static String findNotificationSubject(Message<?> message) {
		Object notificationSubjectHeader = message.getHeaders().get(NOTIFICATION_SUBJECT_HEADER);
		return notificationSubjectHeader != null ? notificationSubjectHeader.toString() : null;
	}

	@Override
	protected boolean sendInternal(Message<?> message, long timeout) {
		PublishRequest publishRequest = toPublishRequest(message);
		SnsPublishObserver observer = this.publishObserver;
		if (observer == null) {
			this.snsClient.publish(publishRequest);
			return true;
		}
		SnsPublishObserver.Publication publication = observer.start(message, this.topicArn.resourceAsString());
		try {
			PublishResponse response = this.snsClient.publish(publication.withTraceContext(publishRequest));
			publication.published(response.messageId());
		}
		catch (RuntimeException ex) {
			publication.failed(ex);
			throw ex;
		}
		finally {
			publication.stop();
		}
		return true;
	}

	private PublishRequest toPublishRequest(Message<?> message) {
		PublishRequest.Builder publishRequestBuilder = PublishRequest.builder();
		publishRequestBuilder.topicArn(this.topicArn.toString()).message(message.getPayload().toString())
				.subject(findNotificationSubject(message));
		Map<String, MessageAttributeValue> messageAttributes = SnsHeaderConverterUtil.toSnsMessageAttributes(message);

		if (!messageAttributes.isEmpty()) {
			publishRequestBuilder.messageAttributes(messageAttributes);
		}
		Optional.ofNullable(message.getHeaders().get(MESSAGE_GROUP_ID_HEADER, String.class))
				.ifPresent(publishRequestBuilder::messageGroupId);
		Optional.ofNullable(message.getHeaders().get(MESSAGE_DEDUPLICATION_ID_HEADER, String.class))
				.ifPresent(publishRequestBuilder::messageDeduplicationId);
		return publishRequestBuilder.build();
	}

}
