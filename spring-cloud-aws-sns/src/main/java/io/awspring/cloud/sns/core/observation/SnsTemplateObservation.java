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

import static io.awspring.cloud.sns.core.SnsHeaders.MESSAGE_DEDUPLICATION_ID_HEADER;
import static io.awspring.cloud.sns.core.SnsHeaders.MESSAGE_GROUP_ID_HEADER;

import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;
import io.micrometer.observation.transport.SenderContext;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Observation for the messages published to SNS by {@link io.awspring.cloud.sns.core.SnsTemplate} and
 * {@link io.awspring.cloud.sns.core.async.SnsAsyncTemplate}.
 * <p>
 * The names and the keys are consistent with the observation of the SQS templates.
 *
 * @author Sharang Gupta
 * @since 4.3.0
 */
public final class SnsTemplateObservation {

	private SnsTemplateObservation() {
	}

	/**
	 * {@link ObservationDocumentation} for the messages published to SNS.
	 */
	public enum Documentation implements ObservationDocumentation {

		/**
		 * Observation of a message that is published to an SNS topic.
		 */
		TEMPLATE {
			@Override
			public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
				return DefaultConvention.class;
			}

			@Override
			public KeyName[] getLowCardinalityKeyNames() {
				return LowCardinalityKeyNames.values();
			}

			@Override
			public KeyName[] getHighCardinalityKeyNames() {
				return HighCardinalityKeyNames.values();
			}
		};

		/**
		 * Low cardinality keys.
		 */
		public enum LowCardinalityKeyNames implements KeyName {

			/**
			 * Messaging system.
			 */
			MESSAGING_SYSTEM("messaging.system"),

			/**
			 * Messaging operation.
			 */
			MESSAGING_OPERATION("messaging.operation"),

			/**
			 * Name of the topic the message is published to.
			 */
			MESSAGING_DESTINATION_NAME("messaging.destination.name"),

			/**
			 * Kind of the destination.
			 */
			MESSAGING_DESTINATION_KIND("messaging.destination.kind");

			private final String key;

			LowCardinalityKeyNames(String key) {
				this.key = key;
			}

			@Override
			public String asString() {
				return this.key;
			}

		}

		/**
		 * High cardinality keys.
		 */
		public enum HighCardinalityKeyNames implements KeyName {

			/**
			 * Id of the published message, as returned by SNS.
			 */
			MESSAGE_ID("messaging.message.id"),

			/**
			 * Message group id of a message published to a FIFO topic.
			 */
			MESSAGE_GROUP_ID("messaging.message.message-group.id"),

			/**
			 * Message deduplication id of a message published to a FIFO topic.
			 */
			MESSAGE_DEDUPLICATION_ID("messaging.message.message-deduplication.id");

			private final String key;

			HighCardinalityKeyNames(String key) {
				this.key = key;
			}

			@Override
			public String asString() {
				return this.key;
			}

		}

	}

	/**
	 * {@link ObservationConvention} for the messages published to SNS. Provide an implementation to the templates to
	 * customize the name or the key values of the observation.
	 */
	public interface Convention extends ObservationConvention<Context> {

		@Override
		default boolean supportsContext(Observation.Context context) {
			return context instanceof Context;
		}

	}

	/**
	 * {@link SenderContext} for the messages published to SNS. The carrier holds the keys of the trace context that the
	 * tracing handlers propagate, which are sent as message attributes.
	 */
	public static class Context extends SenderContext<Map<String, Object>> {

		private static final String BAGGAGE_KEY = "baggage";

		private static final String TRACEPARENT_KEY = "traceparent";

		private static final String TRACESTATE_KEY = "tracestate";

		private static final String B3_KEY = "b3";

		private final Message<?> message;

		private final String topicName;

		private volatile @Nullable String publishedMessageId;

		/**
		 * Create a context for a message that is published to a topic.
		 *
		 * @param message the message to publish.
		 * @param topicName the name of the topic.
		 */
		public Context(Message<?> message, String topicName) {
			super((carrier, key, value) -> {
				if (carrier != null && isPropagatedKey(key)) {
					carrier.put(key, value);
				}
			});
			Assert.notNull(message, "message must not be null");
			Assert.notNull(topicName, "topicName must not be null");
			setCarrier(new HashMap<>());
			setRemoteServiceName("AWS SNS");
			this.message = message;
			this.topicName = topicName;
		}

		private static boolean isPropagatedKey(String key) {
			return BAGGAGE_KEY.equals(key) || TRACEPARENT_KEY.equals(key) || TRACESTATE_KEY.equals(key)
					|| B3_KEY.equals(key);
		}

		/**
		 * Return the message that is published.
		 *
		 * @return the message.
		 */
		public Message<?> getMessage() {
			return this.message;
		}

		/**
		 * Return the name of the topic the message is published to.
		 *
		 * @return the topic name.
		 */
		public String getTopicName() {
			return this.topicName;
		}

		/**
		 * Return the id that SNS assigned to the published message.
		 *
		 * @return the id of the published message, or {@code null} until the message was published.
		 */
		public @Nullable String getPublishedMessageId() {
			return this.publishedMessageId;
		}

		/**
		 * Set the id that SNS assigned to the published message.
		 *
		 * @param publishedMessageId the id of the published message.
		 */
		public void setPublishedMessageId(@Nullable String publishedMessageId) {
			this.publishedMessageId = publishedMessageId;
		}

	}

	/**
	 * Default {@link Convention}.
	 */
	public static class DefaultConvention implements Convention {

		@Override
		public String getName() {
			return "spring.aws.sns.template";
		}

		@Override
		public String getContextualName(Context context) {
			return context.getTopicName() + " send";
		}

		@Override
		public KeyValues getLowCardinalityKeyValues(Context context) {
			return KeyValues
					.of(Documentation.LowCardinalityKeyNames.MESSAGING_SYSTEM.withValue("sns"),
							Documentation.LowCardinalityKeyNames.MESSAGING_OPERATION.withValue("publish"),
							Documentation.LowCardinalityKeyNames.MESSAGING_DESTINATION_NAME
									.withValue(context.getTopicName()),
							Documentation.LowCardinalityKeyNames.MESSAGING_DESTINATION_KIND.withValue("topic"))
					.and(getCustomLowCardinalityKeyValues(context));
		}

		@Override
		public KeyValues getHighCardinalityKeyValues(Context context) {
			KeyValues keyValues = KeyValues.empty();
			String publishedMessageId = context.getPublishedMessageId();
			if (publishedMessageId != null) {
				keyValues = keyValues
						.and(Documentation.HighCardinalityKeyNames.MESSAGE_ID.withValue(publishedMessageId));
			}
			String groupId = headerAsString(context.getMessage(), MESSAGE_GROUP_ID_HEADER);
			if (StringUtils.hasText(groupId)) {
				keyValues = keyValues.and(Documentation.HighCardinalityKeyNames.MESSAGE_GROUP_ID.withValue(groupId));
			}
			String deduplicationId = headerAsString(context.getMessage(), MESSAGE_DEDUPLICATION_ID_HEADER);
			if (StringUtils.hasText(deduplicationId)) {
				keyValues = keyValues
						.and(Documentation.HighCardinalityKeyNames.MESSAGE_DEDUPLICATION_ID.withValue(deduplicationId));
			}
			return keyValues.and(getCustomHighCardinalityKeyValues(context));
		}

		/**
		 * Return custom low cardinality key values, to be overridden by subclasses.
		 *
		 * @param context the context of the observation.
		 * @return key values to add to the observation, empty by default.
		 */
		protected KeyValues getCustomLowCardinalityKeyValues(Context context) {
			return KeyValues.empty();
		}

		/**
		 * Return custom high cardinality key values, to be overridden by subclasses.
		 *
		 * @param context the context of the observation.
		 * @return key values to add to the observation, empty by default.
		 */
		protected KeyValues getCustomHighCardinalityKeyValues(Context context) {
			return KeyValues.empty();
		}

		private static @Nullable String headerAsString(Message<?> message, String headerName) {
			Object value = message.getHeaders().get(headerName);
			return value != null ? value.toString() : null;
		}

	}

}
