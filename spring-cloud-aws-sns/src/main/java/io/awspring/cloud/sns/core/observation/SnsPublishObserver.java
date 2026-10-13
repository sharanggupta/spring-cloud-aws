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
package io.awspring.cloud.sns.core.observation;

import io.awspring.cloud.sns.core.MessageAttributeDataTypes;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * Observes the messages that the SNS templates publish: it starts an observation per published message, sends the
 * propagated trace context as message attributes, and completes the observation with the outcome of the publish.
 *
 * @author Sharang Gupta
 * @since 4.3.0
 */
public final class SnsPublishObserver {

	/**
	 * The maximum number of message attributes SNS accepts for a message.
	 */
	static final int MAX_MESSAGE_ATTRIBUTES = 10;

	private static final Log logger = LogFactory.getLog(SnsPublishObserver.class);

	private static final SnsTemplateObservation.DefaultConvention DEFAULT_CONVENTION = new SnsTemplateObservation.DefaultConvention();

	private final ObservationRegistry observationRegistry;

	private final SnsTemplateObservation.@Nullable Convention customConvention;

	/**
	 * Create an observer.
	 *
	 * @param observationRegistry the registry the observations are recorded with.
	 * @param customConvention the convention to use instead of the default one, if any.
	 */
	public SnsPublishObserver(ObservationRegistry observationRegistry,
			SnsTemplateObservation.@Nullable Convention customConvention) {
		Assert.notNull(observationRegistry, "observationRegistry must not be null");
		this.observationRegistry = observationRegistry;
		this.customConvention = customConvention;
	}

	/**
	 * Create an observer for the templates, if observations are recorded.
	 *
	 * @param observationRegistry the registry the observations are recorded with.
	 * @param customConvention the convention to use instead of the default one, if any.
	 * @return the observer, or {@code null} if the registry does not record observations.
	 */
	public static @Nullable SnsPublishObserver create(ObservationRegistry observationRegistry,
			SnsTemplateObservation.@Nullable Convention customConvention) {
		return observationRegistry.isNoop() ? null : new SnsPublishObserver(observationRegistry, customConvention);
	}

	/**
	 * Start the observation of a message that is about to be published. The observation is a child of the observation
	 * that is current on the calling thread, if there is one.
	 *
	 * @param message the message to publish.
	 * @param topicName the name of the topic the message is published to.
	 * @return the started publication, to be completed once the message was published or failed.
	 */
	public Publication start(Message<?> message, String topicName) {
		SnsTemplateObservation.Context context = new SnsTemplateObservation.Context(message, topicName);
		Observation observation = SnsTemplateObservation.Documentation.TEMPLATE
				.observation(this.customConvention, DEFAULT_CONVENTION, () -> context, this.observationRegistry)
				.start();
		return new Publication(observation, context);
	}

	/**
	 * A published message that is observed.
	 */
	public static final class Publication {

		private final Observation observation;

		private final SnsTemplateObservation.Context context;

		private Publication(Observation observation, SnsTemplateObservation.Context context) {
			this.observation = observation;
			this.context = context;
		}

		/**
		 * Return the request with the trace context of the observation added as message attributes. The attributes that
		 * the request already has are not replaced, and none is added if the request would exceed the maximum number of
		 * message attributes of SNS.
		 *
		 * @param request the request to publish.
		 * @return the request to publish with the trace context.
		 */
		public PublishRequest withTraceContext(PublishRequest request) {
			Map<String, Object> carrier = this.context.getCarrier();
			if (carrier == null || carrier.isEmpty()) {
				return request;
			}
			Map<String, MessageAttributeValue> attributes = new HashMap<>(request.messageAttributes());
			for (Map.Entry<String, Object> entry : carrier.entrySet()) {
				if (attributes.containsKey(entry.getKey())) {
					continue;
				}
				if (attributes.size() >= MAX_MESSAGE_ATTRIBUTES) {
					logger.debug("The message attribute '" + entry.getKey() + "' with the trace context was not added "
							+ "because SNS accepts at most " + MAX_MESSAGE_ATTRIBUTES + " message attributes.");
					continue;
				}
				attributes.put(entry.getKey(), MessageAttributeValue.builder()
						.dataType(MessageAttributeDataTypes.STRING).stringValue(entry.getValue().toString()).build());
			}
			return request.toBuilder().messageAttributes(attributes).build();
		}

		/**
		 * Record the id that SNS assigned to the published message.
		 *
		 * @param messageId the id of the published message.
		 */
		public void published(@Nullable String messageId) {
			this.context.setPublishedMessageId(messageId);
		}

		/**
		 * Record that publishing the message failed.
		 *
		 * @param error the failure.
		 */
		public void failed(Throwable error) {
			this.observation.error(error);
		}

		/**
		 * Stop the observation.
		 */
		public void stop() {
			this.observation.stop();
		}

	}

}
