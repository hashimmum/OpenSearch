/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.Scheduler;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A processor implementation may modify the request or response from a search call.
 * Whether changes are made and what exactly is modified is up to the implementation.
 * <p>
 * Processors may get called concurrently and thus need to be thread-safe.
 *
 * TODO: Refactor {@link org.opensearch.ingest.Processor} to extend this interface, and specialize to IngestProcessor.
 *
 * @opensearch.internal
 */
public interface Processor {
    /**
     * Processor configuration key to let the factory know the context for pipeline creation.
     * <p>
     * See {@link PipelineSource}.
     */
    String PIPELINE_SOURCE = "pipeline_source";

    /**
     * Gets the type of processor
     */
    String getType();

    /**
     * Gets the tag of a processor.
     */
    String getTag();

    /**
     * Gets the description of a processor.
     */
    String getDescription();

    /**
     * A factory that knows how to construct a processor based on a map of maps.
     */
    interface Factory<T extends Processor> {

        /**
         * Creates a processor based on the specified map of maps config.
         *
         * @param processorFactories Other processors which may be created inside this processor
         * @param tag                The tag for the processor
         * @param description        A short description of what this processor does
         * @param config             The configuration for the processor
         *                           <b>Note:</b> Implementations are responsible for removing the used configuration
         *                           keys, so that after creation the config map should be empty.
         * @param pipelineContext    Contextual information about the enclosing pipeline.
         */
        T create(
            Map<String, Factory<T>> processorFactories,
            String tag,
            String description,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception;
    }

    /**
     * Contextual information about the enclosing pipeline. A processor factory may change processor initialization behavior or
     * pass this information to the created processor instance.
     */
    class PipelineContext {
        private final PipelineSource pipelineSource;

        public PipelineContext(PipelineSource pipelineSource) {
            this.pipelineSource = pipelineSource;
        }

        public PipelineSource getPipelineSource() {
            return pipelineSource;
        }
    }

    /**
     * Infrastructure class that holds services that can be used by processor factories to create processor instances
     * and that gets passed around to all {@link SearchPipelinePlugin}s.
     */
    class Parameters {

        /**
         * Useful to provide access to the node's environment like config directory to processor factories.
         */
        public final Environment env;

        /**
         * Provides processors script support.
         */
        public final ScriptService scriptService;

        /**
         * Provide analyzer support
         */
        public final AnalysisRegistry analysisRegistry;

        /**
         * Allows processors to read headers set by {@link org.opensearch.action.support.ActionFilter}
         * instances that have run while handling the current search.
         */
        public final ThreadContext threadContext;

        public final LongSupplier relativeTimeSupplier;

        public final SearchPipelineService searchPipelineService;

        public final Consumer<Runnable> genericExecutor;

        public final NamedXContentRegistry namedXContentRegistry;

        /**
         * Provides scheduler support
         */
        public final BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler;

        /**
         * Provides access to the node's cluster client
         */
        public final Client client;

        public Parameters(
            Environment env,
            ScriptService scriptService,
            AnalysisRegistry analysisRegistry,
            ThreadContext threadContext,
            LongSupplier relativeTimeSupplier,
            BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler,
            SearchPipelineService searchPipelineService,
            Client client,
            Consumer<Runnable> genericExecutor,
            NamedXContentRegistry namedXContentRegistry
        ) {
            this.env = env;
            this.scriptService = scriptService;
            this.threadContext = threadContext;
            this.analysisRegistry = analysisRegistry;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
            this.searchPipelineService = searchPipelineService;
            this.client = client;
            this.genericExecutor = genericExecutor;
            this.namedXContentRegistry = namedXContentRegistry;
        }

    }

    /**
     * Passed via the "pipeline_source" configuration to a processor factory to convey the context for pipeline creation.
     * <p>
     * A processor factory may change the processor initialization behavior based on the creation context (e.g. avoiding
     * creating expensive resources during validation or in a request-scoped pipeline.)
     */
    enum PipelineSource {
        // A named pipeline is being created or updated
        UPDATE_PIPELINE,
        // Pipeline is defined within a search request
        SEARCH_REQUEST,
        // A named pipeline is being validated before being written to cluster state
        VALIDATE_PIPELINE
    }
}
