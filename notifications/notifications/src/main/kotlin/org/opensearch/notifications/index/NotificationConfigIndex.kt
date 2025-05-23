/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.notifications.index

import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.commons.notifications.model.NotificationConfigSearchResult
import org.opensearch.commons.notifications.model.SearchResults
import org.opensearch.commons.utils.logger
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.notifications.NotificationPlugin.Companion.LOG_PREFIX
import org.opensearch.notifications.index.ConfigQueryHelper.getSortField
import org.opensearch.notifications.model.DocInfo
import org.opensearch.notifications.model.DocMetadata.Companion.ACCESS_LIST_TAG
import org.opensearch.notifications.model.DocMetadata.Companion.METADATA_TAG
import org.opensearch.notifications.model.NotificationConfigDoc
import org.opensearch.notifications.model.NotificationConfigDocInfo
import org.opensearch.notifications.settings.PluginSettings
import org.opensearch.notifications.util.SecureIndexClient
import org.opensearch.notifications.util.SuspendUtils.Companion.suspendUntil
import org.opensearch.notifications.util.SuspendUtils.Companion.suspendUntilTimeout
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.script.Script
import org.opensearch.search.SearchHit
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import org.opensearch.transport.client.Client
import java.lang.IllegalArgumentException
import java.util.concurrent.TimeUnit

/**
 * Class for doing index operations to maintain configurations in cluster.
 */
@Suppress("TooManyFunctions")
internal object NotificationConfigIndex : ConfigOperations {
    const val DEFAULT_SCHEMA_VERSION = 1
    const val _META = "_meta"
    const val SCHEMA_VERSION = "schema_version"

    private val log by logger(NotificationConfigIndex::class.java)
    const val INDEX_NAME = ".opensearch-notifications-config"
    private const val MAPPING_FILE_NAME = "notifications-config-mapping.yml"
    private const val SETTINGS_FILE_NAME = "notifications-config-settings.yml"

    private val indexMappingAsMap = XContentHelper.convertToMap(
        XContentType.YAML.xContent(),
        NotificationConfigIndex::class.java.classLoader.getResource(MAPPING_FILE_NAME)?.readText()!!,
        false
    )
    private val indexMappingSchemaVersion = getSchemaVersionFromIndexMapping(indexMappingAsMap)

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var sdkClient: SdkClient

    private val searchHitParser = object : SearchResults.SearchHitParser<NotificationConfigInfo> {
        override fun parse(searchHit: SearchHit): NotificationConfigInfo {
            val parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                searchHit.sourceAsString
            )
            parser.nextToken()
            val doc = NotificationConfigDoc.parse(parser)
            return NotificationConfigInfo(
                searchHit.id,
                doc.metadata.lastUpdateTime,
                doc.metadata.createdTime,
                doc.config
            )
        }
    }

    /**
     * {@inheritDoc}
     */
    fun initialize(sdkClient: SdkClient, client: Client, clusterService: ClusterService) {
        NotificationConfigIndex.client = SecureIndexClient(client)
        NotificationConfigIndex.clusterService = clusterService
        NotificationConfigIndex.sdkClient = sdkClient
    }

    private fun getSchemaVersionFromIndexMapping(indexMapping: Map<String, Any>?): Int {
        var schemaVersion = DEFAULT_SCHEMA_VERSION
        if (indexMapping != null && indexMapping.containsKey(_META) && indexMapping[_META] is Map<*, *>) {
            val metaData = indexMapping[_META] as Map<*, *>
            if (metaData.containsKey(SCHEMA_VERSION)) {
                try {
                    schemaVersion = metaData[SCHEMA_VERSION] as Int
                } catch (e: Exception) {
                    throw IllegalArgumentException("schema_version can only be Integer")
                }
            }
        }
        return schemaVersion
    }

    /**
     * Create index using the mapping and settings defined in resource
     */
    @Suppress("TooGenericExceptionCaught")
    private suspend fun createIndex() {
        if (!isIndexExists()) {
            val classLoader = NotificationConfigIndex::class.java.classLoader
            val indexSettingsSource = classLoader.getResource(SETTINGS_FILE_NAME)?.readText()!!
            val request = CreateIndexRequest(INDEX_NAME)
                .mapping(indexMappingAsMap)
                .settings(indexSettingsSource, XContentType.YAML)
            client.threadPool().threadContext.stashContext().use {
                try {
                    val response: CreateIndexResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
                        admin().indices().create(request, it)
                    }
                    if (response.isAcknowledged) {
                        log.info("$LOG_PREFIX:Index $INDEX_NAME creation Acknowledged")
                    } else {
                        throw IllegalStateException("$LOG_PREFIX:Index $INDEX_NAME creation not Acknowledged")
                    }
                } catch (exception: Exception) {
                    if (exception !is ResourceAlreadyExistsException && exception.cause !is ResourceAlreadyExistsException) {
                        throw exception
                    }
                }
            }
        } else {
            val currentIndexMappingMetadata = clusterService.state().metadata.indices[INDEX_NAME]?.mapping()?.sourceAsMap()
            val currentIndexMappingSchemaVersion = getSchemaVersionFromIndexMapping(currentIndexMappingMetadata)
            if (currentIndexMappingSchemaVersion < indexMappingSchemaVersion) {
                val putMappingRequest: PutMappingRequest = PutMappingRequest(INDEX_NAME).source(indexMappingAsMap)
                client.threadPool().threadContext.stashContext().use {
                    val response: AcknowledgedResponse = client.suspendUntil {
                        admin().indices().putMapping(putMappingRequest, it)
                    }
                    if (response.isAcknowledged) {
                        log.info("$LOG_PREFIX:Index $INDEX_NAME update mapping Acknowledged")
                    } else {
                        throw IllegalStateException("$LOG_PREFIX:Index $INDEX_NAME update mapping not Acknowledged")
                    }
                }
            }
        }
    }

    /**
     * Check if the index is created and available.
     * @return true if index is available, false otherwise
     */
    private fun isIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(INDEX_NAME)
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun createNotificationConfig(configDoc: NotificationConfigDoc, id: String?): String? {
        createIndex()
        val indexRequest = IndexRequest(INDEX_NAME)
            .source(configDoc.toXContent())
            .create(true)
        if (id != null) {
            indexRequest.id(id)
        }
        val response: IndexResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
            index(indexRequest, it)
        }
        return if (response.result != DocWriteResponse.Result.CREATED) {
            log.warn("$LOG_PREFIX:createNotificationConfig - response:$response")
            null
        } else {
            response.id
        }
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun getNotificationConfigs(ids: Set<String>): List<NotificationConfigDocInfo> {
        createIndex()
        val getRequest = MultiGetRequest()
        ids.forEach { getRequest.add(INDEX_NAME, it) }
        val response: MultiGetResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
            multiGet(getRequest, it)
        }
        return response.responses.mapNotNull { parseNotificationConfigDoc(it.id, it.response) }
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun getNotificationConfig(id: String): NotificationConfigDocInfo? {
        createIndex()
        val getRequest = GetRequest(INDEX_NAME).id(id)
        val response: GetResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
            get(getRequest, it)
        }
        return parseNotificationConfigDoc(id, response)
    }

    private fun parseNotificationConfigDoc(id: String, response: GetResponse): NotificationConfigDocInfo? {
        return if (response.sourceAsString == null) {
            log.warn("$LOG_PREFIX:getNotificationConfig - $id not found; response:$response")
            null
        } else {
            val parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                response.sourceAsString
            )
            parser.nextToken()
            val doc = NotificationConfigDoc.parse(parser)
            val info = DocInfo(
                id = id,
                version = response.version,
                seqNo = response.seqNo,
                primaryTerm = response.primaryTerm
            )
            NotificationConfigDocInfo(info, doc)
        }
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun getAllNotificationConfigs(
        access: List<String>,
        request: GetNotificationConfigRequest
    ): NotificationConfigSearchResult {
        createIndex()
        val sourceBuilder = SearchSourceBuilder()
            .timeout(TimeValue(PluginSettings.operationTimeoutMs, TimeUnit.MILLISECONDS))
            .sort(getSortField(request.sortField), request.sortOrder ?: SortOrder.ASC)
            .size(request.maxItems)
            .from(request.fromIndex)
        val query = QueryBuilders.boolQuery()
        if (access.isNotEmpty()) {
            // We consider empty access documents public, so will fetch them as well
            val filterQuery = BoolQueryBuilder().should(
                QueryBuilders.termsQuery("$METADATA_TAG.$ACCESS_LIST_TAG", access)
            ).should(
                QueryBuilders.scriptQuery(Script("doc['$METADATA_TAG.$ACCESS_LIST_TAG'] == []"))
            )
            query.filter(filterQuery)
        }
        ConfigQueryHelper.addQueryFilters(query, request.filterParams)
        sourceBuilder.query(query)
        val searchRequest = SearchRequest()
            .indices(INDEX_NAME)
            .source(sourceBuilder)
        val response: SearchResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
            search(searchRequest, it)
        }
        val result = NotificationConfigSearchResult(request.fromIndex.toLong(), response, searchHitParser)
        log.info(
            "$LOG_PREFIX:getAllNotificationConfigs from:${request.fromIndex}, maxItems:${request.maxItems}," +
                " sortField:${request.sortField}, sortOrder=${request.sortOrder}, filters=${request.filterParams}" +
                " retCount:${result.objectList.size}, totalCount:${result.totalHits}"
        )
        return result
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun updateNotificationConfig(id: String, notificationConfigDoc: NotificationConfigDoc): Boolean {
        createIndex()
        val indexRequest = IndexRequest(INDEX_NAME)
            .source(notificationConfigDoc.toXContent())
            .create(false)
            .id(id)
        val response: IndexResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
            index(indexRequest, it)
        }
        if (response.result != DocWriteResponse.Result.UPDATED) {
            log.warn("$LOG_PREFIX:updateNotificationConfig failed for $id; response:$response")
        }
        return response.result == DocWriteResponse.Result.UPDATED
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun deleteNotificationConfig(id: String): Boolean {
        createIndex()
        val deleteRequest = DeleteRequest()
            .index(INDEX_NAME)
            .id(id)

        val response: DeleteResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
            delete(deleteRequest, it)
        }
        if (response.result != DocWriteResponse.Result.DELETED) {
            log.warn("$LOG_PREFIX:deleteNotificationConfig failed for $id; response:$response")
        }
        return response.result == DocWriteResponse.Result.DELETED
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun deleteNotificationConfigs(ids: Set<String>): Map<String, RestStatus> {
        createIndex()
        val bulkRequest = BulkRequest()
        ids.forEach {
            val deleteRequest = DeleteRequest()
                .index(INDEX_NAME)
                .id(it)
            bulkRequest.add(deleteRequest)
        }
        val response: BulkResponse = client.suspendUntilTimeout(PluginSettings.operationTimeoutMs) {
            bulk(bulkRequest, it)
        }
        val mutableMap = mutableMapOf<String, RestStatus>()
        response.forEach {
            mutableMap[it.id] = it.status()
            if (it.isFailed) {
                log.warn("$LOG_PREFIX:deleteNotificationConfig failed for ${it.id}; response:${it.failureMessage}")
            }
        }
        return mutableMap
    }
}

/**
 * Executes the given [block] function on this resource and then closes it down correctly whether an exception
 * is thrown or not.
 *
 * In case if the resource is being closed due to an exception occurred in [block], and the closing also fails with an exception,
 * the latter is added to the [suppressed][java.lang.Throwable.addSuppressed] exceptions of the former.
 *
 * @param block a function to process this [AutoCloseable] resource.
 * @return the result of [block] function invoked on this resource.
 */
private inline fun <T : ThreadContext.StoredContext, R> T.use(block: (T) -> R): R {
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (e: Throwable) {
        exception = e
        throw e
    } finally {
        closeFinally(exception)
    }
}

/**
 * Closes this [AutoCloseable], suppressing possible exception or error thrown by [AutoCloseable.close] function when
 * it's being closed due to some other [cause] exception occurred.
 *
 * The suppressed exception is added to the list of suppressed exceptions of [cause] exception.
 */
private fun ThreadContext.StoredContext.closeFinally(cause: Throwable?) = when (cause) {
    null -> close()
    else -> try {
        close()
    } catch (closeException: Throwable) {
        cause.addSuppressed(closeException)
    }
}
