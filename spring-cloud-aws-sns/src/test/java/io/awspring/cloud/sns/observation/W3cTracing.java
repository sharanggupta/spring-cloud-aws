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
package io.awspring.cloud.sns.observation;

import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleTracer;
import java.util.List;

/**
 * A {@link TestObservationRegistry} with tracing handlers that inject a W3C {@code traceparent} into the carrier of the
 * sender contexts, so that tests can verify what is propagated with a message.
 *
 * @author Sharang Gupta
 */
public final class W3cTracing {

	private final SimpleTracer tracer = new SimpleTracer();

	private final TestObservationRegistry observationRegistry = TestObservationRegistry.create();

	public W3cTracing() {
		Propagator propagator = new W3cPropagator(this.tracer);
		this.observationRegistry.observationConfig().observationHandler(new DefaultTracingObservationHandler(tracer));
		this.observationRegistry.observationConfig()
				.observationHandler(new PropagatingSenderTracingObservationHandler<>(tracer, propagator));
	}

	public TestObservationRegistry observationRegistry() {
		return this.observationRegistry;
	}

	/**
	 * Return the trace id of the span that is current on the calling thread.
	 *
	 * @return the trace id.
	 */
	public String currentTraceId() {
		return this.tracer.currentTraceContext().context().traceId();
	}

	private record W3cPropagator(Tracer tracer) implements Propagator {

	@Override
	public List<String> fields() {
		return List.of("traceparent");
	}

	@Override
	public <C> void inject(TraceContext context, C carrier, Setter<C> setter) {
		setter.set(carrier, "traceparent", "00-%s-%s-01".formatted(context.traceId(), context.spanId()));
	}

	@Override
	public <C> Span.Builder extract(C carrier, Getter<C> getter) {
		return this.tracer.spanBuilder().setNoParent();
	}

}

}
