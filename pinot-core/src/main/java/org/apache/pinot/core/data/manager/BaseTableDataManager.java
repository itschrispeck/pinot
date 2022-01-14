/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Pair;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseTableDataManager implements TableDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTableDataManager.class);

  protected final ConcurrentHashMap<String, SegmentDataManager> _segmentDataManagerMap = new ConcurrentHashMap<>();

  protected TableDataManagerConfig _tableDataManagerConfig;
  protected String _instanceId;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected ServerMetrics _serverMetrics;
  protected String _tableNameWithType;
  protected String _tableDataDir;
  protected File _indexDir;
  protected File _resourceTmpDir;
  protected Logger _logger;
  protected HelixManager _helixManager;
  protected String _authToken;

  // Fixed size LRU cache with TableName - SegmentName pair as key, and segment related
  // errors as the value.
  protected LoadingCache<Pair<String, String>, SegmentErrorInfo> _errorCache;

  @Override
  public void init(TableDataManagerConfig tableDataManagerConfig, String instanceId,
      ZkHelixPropertyStore<ZNRecord> propertyStore, ServerMetrics serverMetrics, HelixManager helixManager,
      @Nullable LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache) {
    LOGGER.info("Initializing table data manager for table: {}", tableDataManagerConfig.getTableName());

    _tableDataManagerConfig = tableDataManagerConfig;
    _instanceId = instanceId;
    _propertyStore = propertyStore;
    _serverMetrics = serverMetrics;
    _helixManager = helixManager;
    _authToken = tableDataManagerConfig.getAuthToken();

    _tableNameWithType = tableDataManagerConfig.getTableName();
    _tableDataDir = tableDataManagerConfig.getDataDir();
    _indexDir = new File(_tableDataDir);
    if (!_indexDir.exists()) {
      Preconditions.checkState(_indexDir.mkdirs());
    }
    _resourceTmpDir = new File(_indexDir, "tmp");
    // This is meant to cleanup temp resources from TableDataManager. But other code using this same
    // directory will have those deleted as well.
    FileUtils.deleteQuietly(_resourceTmpDir);
    if (!_resourceTmpDir.exists()) {
      Preconditions.checkState(_resourceTmpDir.mkdirs());
    }
    _errorCache = errorCache;
    _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + getClass().getSimpleName());

    doInit();

    _logger.info("Initialized table data manager for table: {} with data directory: {}", _tableNameWithType,
        _tableDataDir);
  }

  protected abstract void doInit();

  @Override
  public void start() {
    _logger.info("Starting table data manager for table: {}", _tableNameWithType);
    doStart();
    _logger.info("Started table data manager for table: {}", _tableNameWithType);
  }

  protected abstract void doStart();

  @Override
  public void shutDown() {
    _logger.info("Shutting down table data manager for table: {}", _tableNameWithType);
    doShutdown();
    _logger.info("Shut down table data manager for table: {}", _tableNameWithType);
  }

  protected abstract void doShutdown();

  /**
   * {@inheritDoc}
   * <p>If one segment already exists with the same name, replaces it with the new one.
   * <p>Ensures that reference count of the old segment (if replaced) is reduced by 1, so that the last user of the old
   * segment (or the calling thread, if there are none) remove the segment.
   * <p>The new segment is added with reference count of 1, so that is never removed until a drop command comes through.
   *
   * @param immutableSegment Immutable segment to add
   */
  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} to table: {}", segmentName, _tableNameWithType);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);

    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.put(segmentName, newSegmentManager);
    if (oldSegmentManager == null) {
      _logger.info("Added new immutable segment: {} to table: {}", segmentName, _tableNameWithType);
    } else {
      _logger.info("Replaced immutable segment: {} of table: {}", segmentName, _tableNameWithType);
      releaseSegment(oldSegmentManager);
    }
  }

  @Override
  public void addSegment(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSegment(String segmentName, TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when we get a helix transition to go to offline or dropped state.
   * We need to remove it safely, keeping in mind that there may be queries that are
   * using the segment,
   * @param segmentName name of the segment to remove.
   */
  @Override
  public void removeSegment(String segmentName) {
    _logger.info("Removing segment: {} from table: {}", segmentName, _tableNameWithType);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.remove(segmentName);
    if (segmentDataManager != null) {
      releaseSegment(segmentDataManager);
      _logger.info("Removed segment: {} from table: {}", segmentName, _tableNameWithType);
    } else {
      _logger.info("Failed to find segment: {} in table: {}", segmentName, _tableNameWithType);
    }
  }

  @Override
  public List<SegmentDataManager> acquireAllSegments() {
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (SegmentDataManager segmentDataManager : _segmentDataManagerMap.values()) {
      if (segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
      }
    }
    return segmentDataManagers;
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentNames, List<String> missingSegments) {
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (String segmentName : segmentNames) {
      SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
      if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
      } else {
        missingSegments.add(segmentName);
      }
    }
    return segmentDataManagers;
  }

  @Nullable
  @Override
  public SegmentDataManager acquireSegment(String segmentName) {
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
      return segmentDataManager;
    } else {
      return null;
    }
  }

  @Override
  public void releaseSegment(SegmentDataManager segmentDataManager) {
    if (segmentDataManager.decreaseReferenceCount()) {
      closeSegment(segmentDataManager);
    }
  }

  private void closeSegment(SegmentDataManager segmentDataManager) {
    String segmentName = segmentDataManager.getSegmentName();
    _logger.info("Closing segment: {} of table: {}", segmentName, _tableNameWithType);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, -1L);
    _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_SEGMENT_COUNT, 1L);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        -segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs());
    segmentDataManager.destroy();
    _logger.info("Closed segment: {} of table: {}", segmentName, _tableNameWithType);
  }

  @Override
  public int getNumSegments() {
    return _segmentDataManagerMap.size();
  }

  @Override
  public String getTableName() {
    return _tableNameWithType;
  }

  @Override
  public File getTableDataDir() {
    return _indexDir;
  }

  @Override
  public void addSegmentError(String segmentName, SegmentErrorInfo segmentErrorInfo) {
    _errorCache.put(new Pair<>(_tableNameWithType, segmentName), segmentErrorInfo);
  }

  @Override
  public Map<String, SegmentErrorInfo> getSegmentErrors() {
    if (_errorCache == null) {
      return Collections.emptyMap();
    } else {
      // Filter out entries that match the table name.
      return _errorCache.asMap().entrySet().stream().filter(map -> map.getKey().getFirst().equals(_tableNameWithType))
          .collect(Collectors.toMap(map -> map.getKey().getSecond(), Map.Entry::getValue));
    }
  }

  @Override
  public void reloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      SegmentMetadata localMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    File indexDir = getSegmentDataDir(segmentName);
    try {
      // Create backup directory to handle failure of segment reloading.
      createBackup(indexDir);

      // Download segment from deep store if CRC changes or forced to download;
      // otherwise, copy backup directory back to the original index directory.
      // And then continue to load the segment from the index directory.
      boolean shouldDownload = forceDownload || !hasSameCRC(zkMetadata, localMetadata);
      if (shouldDownload && allowDownload(segmentName, zkMetadata)) {
        if (forceDownload) {
          LOGGER.info("Segment: {} of table: {} is forced to download", segmentName, _tableNameWithType);
        } else {
          LOGGER.info("Download segment:{} of table: {} as crc changes from: {} to: {}", segmentName,
              _tableNameWithType, localMetadata.getCrc(), zkMetadata.getCrc());
        }
        indexDir = downloadSegment(segmentName, zkMetadata);
      } else {
        LOGGER.info("Reload existing segment: {} of table: {}", segmentName, _tableNameWithType);
        // The indexDir is empty after calling createBackup, as it's renamed to a backup directory.
        // The SegmentDirectory should initialize accordingly. Like for SegmentLocalFSDirectory, it
        // doesn't load anything from an empty indexDir, but gets the info to complete the copyTo.
        try (SegmentDirectory segmentDirectory = initSegmentDirectory(segmentName, indexLoadingConfig)) {
          segmentDirectory.copyTo(indexDir);
        }
      }

      // Load from indexDir and replace the old segment in memory. What's inside indexDir
      // may come from SegmentDirectory.copyTo() or the segment downloaded from deep store.
      ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, schema);
      addSegment(segment);

      // Remove backup directory to mark the completion of segment reloading.
      removeBackup(indexDir);
    } catch (Exception reloadFailureException) {
      try {
        LoaderUtils.reloadFailureRecovery(indexDir);
      } catch (Exception recoveryFailureException) {
        LOGGER.error("Failed to recover after reload failure", recoveryFailureException);
        reloadFailureException.addSuppressed(recoveryFailureException);
      }
      throw reloadFailureException;
    }
  }

  @Override
  public void addOrReplaceSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata, @Nullable SegmentMetadata localMetadata)
      throws Exception {
    if (localMetadata != null && hasSameCRC(zkMetadata, localMetadata)) {
      LOGGER.info("Segment: {} of table: {} has crc: {} same as before, already loaded, do nothing", segmentName,
          _tableNameWithType, localMetadata.getCrc());
      return;
    }

    // The segment is not loaded by the server if the metadata object is null. But the segment
    // may still be kept on the server. For example when server gets restarted, the segment is
    // still on the server but the metadata object has not been initialized yet. In this case,
    // we should check if the segment exists on server and try to load it. If the segment does
    // not exist or fails to get loaded, we download segment from deep store to load it again.
    if (localMetadata == null && tryLoadExistingSegment(segmentName, indexLoadingConfig, zkMetadata)) {
      return;
    }

    Preconditions.checkState(allowDownload(segmentName, zkMetadata), "Segment: %s of table: %s does not allow download",
        segmentName, _tableNameWithType);

    // Download segment and replace the local one, either due to failure to recover local segment,
    // or the segment data is updated and has new CRC now.
    if (localMetadata == null) {
      LOGGER.info("Download segment: {} of table: {} as it doesn't exist", segmentName, _tableNameWithType);
    } else {
      LOGGER.info("Download segment: {} of table: {} as crc changes from: {} to: {}", segmentName,
          _tableNameWithType, localMetadata.getCrc(), zkMetadata.getCrc());
    }
    File indexDir = downloadSegment(segmentName, zkMetadata);
    addSegment(indexDir, indexLoadingConfig);
    LOGGER.info("Downloaded and loaded segment: {} of table: {} with crc: {}", segmentName, _tableNameWithType,
        zkMetadata.getCrc());
  }

  protected boolean allowDownload(String segmentName, SegmentZKMetadata zkMetadata) {
    return true;
  }

  protected File downloadSegment(String segmentName, SegmentZKMetadata zkMetadata)
      throws Exception {
    // TODO: may support download from peer servers for RealTime table.
    return downloadSegmentFromDeepStore(segmentName, zkMetadata);
  }

  private File downloadSegmentFromDeepStore(String segmentName, SegmentZKMetadata zkMetadata)
      throws Exception {
    File tempRootDir = getTmpSegmentDataDir("tmp-" + segmentName + "-" + UUID.randomUUID());
    FileUtils.forceMkdir(tempRootDir);
    try {
      File tarFile = downloadAndDecrypt(segmentName, zkMetadata, tempRootDir);
      return untarAndMoveSegment(segmentName, tarFile, tempRootDir);
    } finally {
      FileUtils.deleteQuietly(tempRootDir);
    }
  }

  @VisibleForTesting
  File downloadAndDecrypt(String segmentName, SegmentZKMetadata zkMetadata, File tempRootDir)
      throws Exception {
    File tarFile = new File(tempRootDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    String uri = zkMetadata.getDownloadUrl();
    try {
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(uri, tarFile, zkMetadata.getCrypterName());
      LOGGER.info("Downloaded tarred segment: {} for table: {} from: {} to: {}, file length: {}", segmentName,
          _tableNameWithType, uri, tarFile, tarFile.length());
      return tarFile;
    } catch (AttemptsExceededException e) {
      LOGGER.error("Attempts exceeded when downloading segment: {} for table: {} from: {} to: {}", segmentName,
          _tableNameWithType, uri, tarFile);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  File untarAndMoveSegment(String segmentName, File tarFile, File tempRootDir)
      throws IOException {
    File untarDir = new File(tempRootDir, segmentName);
    try {
      // If an exception is thrown when untarring, it means the tar file is broken
      // or not found after the retry. Thus, there's no need to retry again.
      File untaredSegDir = TarGzCompressionUtils.untar(tarFile, untarDir).get(0);
      LOGGER.info("Uncompressed tar file: {} into target dir: {}", tarFile, untarDir);
      // Replace the existing index directory.
      File indexDir = getSegmentDataDir(segmentName);
      FileUtils.deleteDirectory(indexDir);
      FileUtils.moveDirectory(untaredSegDir, indexDir);
      LOGGER.info("Successfully downloaded segment: {} of table: {} to index dir: {}", segmentName, _tableNameWithType,
          indexDir);
      return indexDir;
    } catch (Exception e) {
      LOGGER.error("Failed to untar segment: {} of table: {} from: {} to: {}", segmentName, _tableNameWithType, tarFile,
          untarDir);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.UNTAR_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  File getSegmentDataDir(String segmentName) {
    return new File(_indexDir, segmentName);
  }

  @VisibleForTesting
  protected File getTmpSegmentDataDir(String segmentName) {
    return new File(_resourceTmpDir, segmentName);
  }

  /**
   * Create a backup directory to handle failure of segment reloading.
   * First rename index directory to segment backup directory so that original segment have all file
   * descriptors point to the segment backup directory to ensure original segment serves queries properly.
   * The original index directory is restored lazily, as depending on the conditions,
   * it may be restored from the backup directory or segment downloaded from deep store.
   */
  private void createBackup(File indexDir) {
    if (!indexDir.exists()) {
      return;
    }
    File parentDir = indexDir.getParentFile();
    File segmentBackupDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    // Rename index directory to segment backup directory (atomic).
    Preconditions.checkState(indexDir.renameTo(segmentBackupDir),
        "Failed to rename index directory: %s to segment backup directory: %s", indexDir, segmentBackupDir);
  }

  /**
   * Remove the backup directory to mark the completion of segment reloading.
   * First rename then delete is as renaming is an atomic operation, but deleting is not.
   * When we rename the segment backup directory to segment temporary directory, we know the reload
   * already succeeded, so that we can safely delete the segment temporary directory.
   */
  private void removeBackup(File indexDir)
      throws IOException {
    File parentDir = indexDir.getParentFile();
    File segmentBackupDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    if (!segmentBackupDir.exists()) {
      return;
    }
    // Rename segment backup directory to segment temporary directory (atomic).
    File segmentTempDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);
    Preconditions.checkState(segmentBackupDir.renameTo(segmentTempDir),
        "Failed to rename segment backup directory: %s to segment temporary directory: %s", segmentBackupDir,
        segmentTempDir);
    FileUtils.deleteDirectory(segmentTempDir);
  }

  /**
   * Try to load the segment potentially still existing on the server.
   *
   * @return true if the segment still exists on server, its CRC is still same with the
   * one in SegmentZKMetadata and is loaded into memory successfully; false if it doesn't
   * exist on the server, its CRC has changed, or it fails to be loaded. SegmentDirectory
   * object may be created when trying to load the segment, but it's closed if the method
   * returns false; otherwise it's opened and to be referred by ImmutableSegment object.
   */
  private boolean tryLoadExistingSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata) {
    // Try to recover the segment from potential segment reloading failure.
    File indexDir = getSegmentDataDir(segmentName);
    recoverReloadFailureQuietly(_tableNameWithType, segmentName, indexDir);

    // Creates the SegmentDirectory object to access the segment metadata.
    // The metadata is null if the segment doesn't exist yet.
    SegmentDirectory segmentDirectory = tryInitSegmentDirectory(segmentName, indexLoadingConfig);
    SegmentMetadataImpl segmentMetadata = (segmentDirectory == null) ? null : segmentDirectory.getSegmentMetadata();

    // If the segment doesn't exist on server or its CRC has changed, then we
    // need to fall back to download the segment from deep store to load it.
    if (segmentMetadata == null || !hasSameCRC(zkMetadata, segmentMetadata)) {
      if (segmentMetadata == null) {
        LOGGER.info("Segment: {} of table: {} does not exist", segmentName, _tableNameWithType);
      } else if (!hasSameCRC(zkMetadata, segmentMetadata)) {
        LOGGER.info("Segment: {} of table: {} has crc change from: {} to: {}", segmentName, _tableNameWithType,
            segmentMetadata.getCrc(), zkMetadata.getCrc());
      }
      closeSegmentDirectoryQuietly(segmentDirectory);
      return false;
    }

    try {
      // If the segment is still kept by the server, then we can
      // either load it directly if it's still consistent with latest table config and schema;
      // or reprocess it to reflect latest table config and schema before loading.
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      if (!ImmutableSegmentLoader.needPreprocess(segmentDirectory, indexLoadingConfig, schema)) {
        LOGGER.info("Segment: {} of table: {} is consistent with latest table config and schema", segmentName,
            _tableNameWithType);
      } else {
        LOGGER.info("Segment: {} of table: {} needs reprocess to reflect latest table config and schema", segmentName,
            _tableNameWithType);
        segmentDirectory.copyTo(indexDir);
        // Close the stale SegmentDirectory object and recreate it with reprocessed segment.
        closeSegmentDirectoryQuietly(segmentDirectory);
        ImmutableSegmentLoader.preprocess(indexDir, indexLoadingConfig, schema);
        segmentDirectory = initSegmentDirectory(segmentName, indexLoadingConfig);
      }
      ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDirectory, indexLoadingConfig, schema);
      addSegment(segment);
      LOGGER.info("Loaded existing segment: {} of table: {} with crc: {}", segmentName, _tableNameWithType,
          zkMetadata.getCrc());
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to load existing segment: {} of table: {} with crc: {}", segmentName, _tableNameWithType, e);
      closeSegmentDirectoryQuietly(segmentDirectory);
      return false;
    }
  }

  private SegmentDirectory tryInitSegmentDirectory(String segmentName, IndexLoadingConfig indexLoadingConfig) {
    try {
      return initSegmentDirectory(segmentName, indexLoadingConfig);
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize SegmentDirectory for segment: {} of table: {} with error: {}", segmentName,
          _tableNameWithType, e.getMessage());
      return null;
    }
  }

  private SegmentDirectory initSegmentDirectory(String segmentName, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    SegmentDirectoryLoaderContext loaderContext =
        new SegmentDirectoryLoaderContext(indexLoadingConfig.getTableConfig(), indexLoadingConfig.getInstanceId(),
            segmentName, indexLoadingConfig.getSegmentDirectoryConfigs());
    SegmentDirectoryLoader segmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    File indexDir = getSegmentDataDir(segmentName);
    return segmentDirectoryLoader.load(indexDir.toURI(), loaderContext);
  }

  private static boolean hasSameCRC(SegmentZKMetadata zkMetadata, SegmentMetadata localMetadata) {
    return zkMetadata.getCrc() == Long.parseLong(localMetadata.getCrc());
  }

  private static void recoverReloadFailureQuietly(String tableNameWithType, String segmentName, File indexDir) {
    try {
      LoaderUtils.reloadFailureRecovery(indexDir);
    } catch (Exception e) {
      LOGGER.warn("Failed to recover segment: {} of table: {} due to error: {}", segmentName, tableNameWithType,
          e.getMessage());
    }
  }

  private static void closeSegmentDirectoryQuietly(SegmentDirectory segmentDirectory) {
    if (segmentDirectory != null) {
      try {
        segmentDirectory.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close SegmentDirectory due to error: {}", e.getMessage());
      }
    }
  }
}
