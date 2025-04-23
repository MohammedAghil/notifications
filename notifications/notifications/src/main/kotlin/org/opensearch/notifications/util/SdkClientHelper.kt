/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.notifications.util

import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import org.opensearch.OpenSearchStatusException
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.remote.metadata.common.SdkClientUtils.unwrapAndConvertToException
import java.util.concurrent.CompletionStage
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Helper class for working with SdkClient operations.
 * Provides methods for executing operations, handling errors, and parsing responses.
 */
class SdkClientHelper {

    /**
     * Parses a raw response into the specified result type.
     *
     * @param raw The raw response to parse
     * @param resultClass The class to parse the result into
     * @return The parsed result
     * @throws IllegalArgumentException if parsing fails
     */
    private fun <Raw : Any, R : Any> parse(raw: Raw, resultClass: Class<R>): R {
        val parser = raw::class.java
            .getMethod("parser")
            .invoke(raw) as? XContentParser
            ?: throw IllegalArgumentException("Missing parser() on ${raw::class.simpleName}")

        val parsed = resultClass
            .getMethod("fromXContent", XContentParser::class.java)
            .invoke(null, parser) as? R
            ?: throw IllegalArgumentException("Failed to parse response into ${resultClass.simpleName}")

        return parsed
    }

    /**
     * Suspends the coroutine until the operation completes or times out.
     * This method is designed to work with CompletionStage-based async operations.
     * If the operation exceeds the timeout, the coroutine is cancelled.
     *
     * @param timeoutMs The timeout in milliseconds
     * @param block The block that returns a CompletionStage
     * @return The result of the operation
     * @throws TimeoutCancellationException if the operation times out
     * @throws OpenSearchStatusException if a version conflict occurs
     */
    suspend fun <T> suspendUntilTimeout(
        timeoutMs: Long,
        block: () -> CompletionStage<T>
    ): T {
        return withTimeout(timeoutMs) {
            suspendCancellableCoroutine { cont ->
                try {
                    block().whenComplete { result, exception ->
                        if (exception != null) {
                            val unwrappedException = unwrapAndConvertToException(exception)
                            val processedException = handleException(unwrappedException)
                            cont.resumeWithException(processedException)
                        } else {
                            cont.resume(result)
                        }
                    }
                } catch (e: Exception) {
                    val processedException = handleException(e)
                    cont.resumeWithException(processedException)
                }
            }
        }
    }

    /**
     * Suspends the coroutine until the operation completes or times out,
     * then parses the result into the specified type.
     * This method is designed to work with CompletionStage-based async operations.
     * If the operation exceeds the timeout, the coroutine is cancelled.
     *
     * @param resultClass The class to parse the result into
     * @param timeoutMs The timeout in milliseconds
     * @param block The block that returns a CompletionStage
     * @return The parsed result of the operation
     * @throws TimeoutCancellationException if the operation times out
     * @throws IllegalArgumentException if parsing fails
     * @throws OpenSearchStatusException if a version conflict occurs
     */
    suspend fun <Raw : Any, R : Any> suspendUntilTimeoutAndParse(
        resultClass: Class<R>,
        timeoutMs: Long,
        block: () -> CompletionStage<Raw>
    ): R {
        val raw = suspendUntilTimeout(timeoutMs, block)
        return parse(raw, resultClass)
    }

    /**
     * Centralized Method for Handling different types of exceptions,
     * transforming them into appropriate OpenSearch exceptions
     *
     * @param exception The exception to process
     * @return The appropriate matching exception found
     */
    private fun handleException(exception: Throwable?): Throwable {
        if (exception == null) return RuntimeException("No Exception provided, Expected an exception to be handled")

        // Check for version conflict
        if (isVersionConflict(exception)) {
            val msg = exception.message ?: "Version conflict occurred"
            return OpenSearchStatusException(msg, RestStatus.CONFLICT)
        }

        return exception
    }

    /**
     * Checks if the exception represents a version conflict
     */
    private fun isVersionConflict(exception: Throwable): Boolean {
        val cause = exception.cause ?: exception
        return cause is VersionConflictEngineException ||
            (cause is OpenSearchStatusException && cause.cause is VersionConflictEngineException)
    }
}
