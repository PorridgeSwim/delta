/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.spark.read;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.types.TypeWideningChecker;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.spark.utils.ScalaUtils;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.spark.utils.StreamingHelper;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.delta.*;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;
import scala.collection.JavaConverters;
import scala.jdk.javaapi.CollectionConverters;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.TYPE_WIDENING_RW_FEATURE;
import static io.delta.kernel.internal.tablefeatures.TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE;

public class SparkMicroBatchStream
    implements MicroBatchStream, SupportsAdmissionControl, SupportsTriggerAvailableNow {

  private static final Logger logger = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private static final Set<DeltaAction> ACTION_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(DeltaAction.ADD, DeltaAction.REMOVE)));

  private final Engine engine;
  private final DeltaSnapshotManager snapshotManager;
  private final DeltaOptions options;
  private final String tableId;
  private final boolean shouldValidateOffsets;
  private final SparkSession spark;
  private final Snapshot snapshotAtSourceInit;
  private final Metadata metadataAtSourceInit;

  // Tracks whether this is the initial batch for this stream (no checkpointed offset).
  private boolean isInitialBatch = false;

  /**
   * Flag that allows user to force enable unsafe streaming read on Delta table with
   * column mapping enabled AND drop/rename actions.
   */
  protected final boolean allowUnsafeStreamingReadOnColumnMappingSchemaChanges;

  protected final boolean allowUnsafeStreamingReadOnPartitionColumnChanges;

  /**
   * Flag that allows user to disable the read-compatibility check during stream start which
   * protects against a corner case in which verifyStreamHygiene could not detect.
   * This is a bug fix but yet a potential behavior change, so we add a flag to fallback.
   */
  protected final boolean forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart;

  /**
   * Flag that allow user to fall back to the legacy behavior in which user can allow nullable=false
   * schema to read nullable=true data, which is incorrect but a behavior change regardless.
   */
  protected final boolean forceEnableUnsafeReadOnNullabilityChange;

  /**
   * Whether we are streaming from a table with column mapping enabled
   */
  protected final boolean isStreamingFromColumnMappingTable;

  /**
   * Whether we are streaming from a table that has the type widening table feature enabled.
   */
  protected final boolean typeWideningEnabled;

  /**
   * Whether we should track widening type changes to allow users to accept them and resume
   * stream processing.
   */
  protected final boolean enableSchemaTrackingForTypeWidening;

  protected final StructType readSchemaAtSourceInit;

  public SparkMicroBatchStream(DeltaSnapshotManager snapshotManager, Configuration hadoopConf) {
    this(
        snapshotManager,
        hadoopConf,
        SparkSession.active(),
        new DeltaOptions(
            scala.collection.immutable.Map$.MODULE$.empty(),
            SparkSession.active().sessionState().conf()));
  }

  public SparkMicroBatchStream(
      DeltaSnapshotManager snapshotManager,
      Configuration hadoopConf,
      SparkSession spark,
      DeltaOptions options) {
    this.spark = spark;
    this.snapshotManager = snapshotManager;
    this.engine = DefaultEngine.create(hadoopConf);
    this.options = options;

    // Initialize snapshot at source init to get table ID, similar to DeltaSource.scala
    this.snapshotAtSourceInit = snapshotManager.loadLatestSnapshot();
    SnapshotImpl snapshotImplAtSourceInit = (SnapshotImpl) snapshotAtSourceInit;
    Protocol protocolAtSourceInit = snapshotImplAtSourceInit.getProtocol();
    this.metadataAtSourceInit = snapshotImplAtSourceInit.getMetadata();
    this.tableId = metadataAtSourceInit.getId();

    this.shouldValidateOffsets =
        (Boolean) spark.sessionState().conf().getConf(DeltaSQLConf.STREAMING_OFFSET_VALIDATION());

    this.allowUnsafeStreamingReadOnColumnMappingSchemaChanges =
        (Boolean) spark.sessionState().conf().getConf(DeltaSQLConf
            .DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES());
    this.allowUnsafeStreamingReadOnPartitionColumnChanges = (Boolean) spark.sessionState().conf()
        .getConf(DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_PARTITION_COLUMN_CHANGE());
    this.forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart =
        (Boolean) spark.sessionState().conf().getConf(DeltaSQLConf
                .DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES_DURING_STREAM_START());
    this.forceEnableUnsafeReadOnNullabilityChange = (Boolean) spark.sessionState().conf()
            .getConf(DeltaSQLConf.DELTA_STREAM_UNSAFE_READ_ON_NULLABILITY_CHANGE());
    this.isStreamingFromColumnMappingTable = ColumnMapping.getColumnMappingMode(
        metadataAtSourceInit.getConfiguration()) != ColumnMapping.ColumnMappingMode.NONE;
    this.typeWideningEnabled = (Boolean) spark.sessionState().conf()
        .getConf(DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE()) &&
            (protocolAtSourceInit.supportsFeature(TYPE_WIDENING_RW_PREVIEW_FEATURE) ||
            protocolAtSourceInit.supportsFeature(TYPE_WIDENING_RW_FEATURE));
    this.enableSchemaTrackingForTypeWidening =
        (Boolean) spark.sessionState().conf()
            .getConf(DeltaSQLConf.DELTA_TYPE_WIDENING_ENABLE_STREAMING_SCHEMA_TRACKING());
    // TODO: Schema tracking
    this.readSchemaAtSourceInit = metadataAtSourceInit.getSchema();
  }

  /**
   * When AvailableNow is used, this offset will be the upper bound where this run of the query will
   * process up. We may run multiple micro batches, but the query will stop itself when it reaches
   * this offset.
   */
  protected Optional<DeltaSourceOffset> lastOffsetForTriggerAvailableNow = Optional.empty();

  private boolean isLastOffsetForTriggerAvailableNowInitialized = false;

  private boolean isTriggerAvailableNow = false;

  @Override
  public void prepareForTriggerAvailableNow() {
    logger.info("The streaming query reports to use Trigger.AvailableNow.");
    isTriggerAvailableNow = true;
  }

  /**
   * initialize the internal states for AvailableNow if this method is called first time after
   * prepareForTriggerAvailableNow.
   */
  protected void initForTriggerAvailableNowIfNeeded(Optional<DeltaSourceOffset> startOffsetOpt) {
    if (isTriggerAvailableNow && !isLastOffsetForTriggerAvailableNowInitialized) {
      isLastOffsetForTriggerAvailableNowInitialized = true;
      initLastOffsetForTriggerAvailableNow(startOffsetOpt);
    }
  }

  protected void initLastOffsetForTriggerAvailableNow(Optional<DeltaSourceOffset> startOffsetOpt) {
    Optional<DeltaSourceOffset> offset =
        latestOffsetInternal(startOffsetOpt, ReadLimit.allAvailable());
    lastOffsetForTriggerAvailableNow = offset;

    lastOffsetForTriggerAvailableNow.ifPresent(
        lastOffset ->
            logger.info("lastOffset for Trigger.AvailableNow has set to " + lastOffset.json()));
  }

  ////////////
  // offset //
  ////////////

  /**
   * Returns the initial offset for a streaming query to start reading from (if there's no
   * checkpointed offset). Returns null if there's no data to read.
   */
  @Override
  public Offset initialOffset() {
    Optional<Long> startingVersionOpt = getStartingVersion();
    long version;
    boolean isInitialSnapshot;

    if (startingVersionOpt.isPresent()) {
      version = startingVersionOpt.get();
      isInitialSnapshot = false;
    } else {
      // TODO(#5318): Support initial snapshot case (isInitialSnapshot == true)
      throw new UnsupportedOperationException(
          "initialOffset with initial snapshot is not supported yet");
    }

    if (version < 0) {
      // This shouldn't happen; defensively return null.
      return null;
    }

    isInitialBatch = true;

    return DeltaSourceOffset.apply(
        tableId, version, DeltaSourceOffset.BASE_INDEX(), isInitialSnapshot);
  }

  @Override
  public Offset latestOffset() {
    throw new IllegalStateException(
        "latestOffset() should not be called - use latestOffset(Offset, ReadLimit) instead");
  }

  /**
   * Get the latest offset with rate limiting (SupportsAdmissionControl).
   *
   * @param startOffset The starting offset (can be null if initialOffset() returned null)
   * @param limit The read limit for rate limiting
   * @return The latest offset, or null if no data is available to read.
   */
  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    // For the first batch, initialOffset() should be called before latestOffset().
    // if startOffset is null: no data is available to read.
    if (startOffset == null) {
      return null;
    }
    Optional<DeltaSourceOffset> deltaStartOffset =
        Optional.of(DeltaSourceOffset.apply(tableId, startOffset));
    initForTriggerAvailableNowIfNeeded(deltaStartOffset);
    DeltaSourceOffset endOffset = latestOffsetInternal(deltaStartOffset, limit).orElse(null);
    isInitialBatch = false;
    return endOffset;
  }

  protected Optional<DeltaSourceOffset> latestOffsetInternal(
      Optional<DeltaSourceOffset> deltaStartOffset, ReadLimit limit) {
    Optional<DeltaSource.AdmissionLimits> limits =
        ScalaUtils.toJavaOptional(DeltaSource.AdmissionLimits$.MODULE$.apply(options, limit));

    Optional<DeltaSourceOffset> endOffset =
        deltaStartOffset.flatMap(offset -> getNextOffsetFromPreviousOffset(offset, limits));

    if (shouldValidateOffsets && deltaStartOffset.isPresent() && endOffset.isPresent()) {
      DeltaSourceOffset.validateOffsets(deltaStartOffset.get(), endOffset.get());
    }

    // endOffset is null: no data is available to read for this batch.
    return endOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    throw new UnsupportedOperationException("deserializeOffset is not supported");
  }

  @Override
  public ReadLimit getDefaultReadLimit() {
    return DeltaSource.AdmissionLimits$.MODULE$.toReadLimit(options);
  }

  /**
   * Return the next offset when previous offset exists. Mimics
   * DeltaSource.getNextOffsetFromPreviousOffset.
   *
   * @param previousOffset The previous offset
   * @param limits Rate limits for this batch (Optional.empty() for no limits)
   * @return The next offset, or the previous offset if no new data is available (except on the
   *     initial batch where we return empty to match DSv1's
   *     getStartingOffsetFromSpecificDeltaVersion behavior)
   */
  private Optional<DeltaSourceOffset> getNextOffsetFromPreviousOffset(
      DeltaSourceOffset previousOffset, Optional<DeltaSource.AdmissionLimits> limits) {
    // TODO(#5319): Special handling for schema tracking.

    CloseableIterator<IndexedFile> changes =
        getFileChangesWithRateLimit(
            previousOffset.reservoirVersion(),
            previousOffset.index(),
            previousOffset.isInitialSnapshot(),
            limits);

    Optional<IndexedFile> lastFileChange = StreamingHelper.iteratorLast(changes);

    if (!lastFileChange.isPresent()) {
      // On the initial batch, return empty to match DSv1's
      // getStartingOffsetFromSpecificDeltaVersion
      if (isInitialBatch) {
        return Optional.empty();
      }
      return Optional.of(previousOffset);
    }
    // TODO(#5318): Check read-incompatible schema changes during stream start
    IndexedFile lastFile = lastFileChange.get();
    return Optional.of(
        DeltaSource.buildOffsetFromIndexedFile(
            tableId,
            lastFile.getVersion(),
            lastFile.getIndex(),
            previousOffset.reservoirVersion(),
            previousOffset.isInitialSnapshot()));
  }

  ////////////
  /// data ///
  ////////////

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    throw new UnsupportedOperationException("planInputPartitions is not supported");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("createReaderFactory is not supported");
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    throw new UnsupportedOperationException("commit is not supported");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop is not supported");
  }

  ///////////////////////
  // getStartingVersion //
  ///////////////////////

  /**
   * Extracts whether users provided the option to time travel a relation. If a query restarts from
   * a checkpoint and the checkpoint has recorded the offset, this method should never be called.
   *
   * <p>Returns Optional.empty() if no starting version is provided.
   *
   * <p>This is the DSv2 Kernel-based implementation of DeltaSource.getStartingVersion.
   */
  Optional<Long> getStartingVersion() {
    // TODO(#5319): DeltaSource.scala uses `allowOutOfRange` parameter from
    // DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.
    if (options.startingVersion().isDefined()) {
      DeltaStartingVersion startingVersion = options.startingVersion().get();
      if (startingVersion instanceof StartingVersionLatest$) {
        Snapshot latestSnapshot = snapshotManager.loadLatestSnapshot();
        // "latest": start reading from the next commit
        return Optional.of(latestSnapshot.getVersion() + 1);
      } else if (startingVersion instanceof StartingVersion) {
        long version = ((StartingVersion) startingVersion).version();
        if (!validateProtocolAt(spark, snapshotManager, engine, version)) {
          // When starting from a given version, we don't require that the snapshot of this
          // version can be reconstructed, even though the input table is technically in an
          // inconsistent state. If the snapshot cannot be reconstructed, then the protocol
          // check is skipped, so this is technically not safe, but we keep it this way for
          // historical reasons.
          snapshotManager.checkVersionExists(
              version, /* mustBeRecreatable= */ false, /* allowOutOfRange= */ false);
        }
        return Optional.of(version);
      }
    }
    // TODO(#5319): Implement startingTimestamp support
    return Optional.empty();
  }

  /**
   * Validate the protocol at a given version. If the snapshot reconstruction fails for any other
   * reason than unsupported feature exception, we suppress it. This allows fallback to previous
   * behavior where the starting version/timestamp was not mandatory to point to reconstructable
   * snapshot.
   *
   * <p>This is the DSv2 Kernel-based implementation of DeltaSource.validateProtocolAt.
   *
   * <p>Returns true when the validation was performed and succeeded.
   */
  private static boolean validateProtocolAt(
      SparkSession spark, DeltaSnapshotManager snapshotManager, Engine engine, long version) {
    boolean alwaysValidateProtocol =
        (Boolean)
            spark
                .sessionState()
                .conf()
                .getConf(DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL());
    if (!alwaysValidateProtocol) {
      return false;
    }

    try {
      // Attempt to construct a snapshot at the startingVersion to validate the protocol
      // If snapshot reconstruction fails, fall back to old behavior where the only
      // requirement was for the commit to exist
      snapshotManager.loadSnapshotAt(version);
      return true;
    } catch (UnsupportedTableFeatureException e) {
      // Re-throw fatal unsupported table feature exceptions
      throw e;
    } catch (Exception e) {
      // Suppress non-fatal exceptions
      logger.warn("Protocol validation failed at version {} with: {}", version, e.getMessage());
      return false;
    }
  }

  ////////////////////
  // getFileChanges //
  ////////////////////

  /**
   * Get file changes with rate limiting applied. Mimics DeltaSource.getFileChangesWithRateLimit.
   *
   * @param fromVersion The starting version (exclusive with fromIndex)
   * @param fromIndex The starting index within fromVersion (exclusive)
   * @param isInitialSnapshot Whether this is the initial snapshot
   * @param limits Rate limits to apply (Optional.empty() for no limits)
   * @return An iterator of IndexedFile with rate limiting applied
   */
  CloseableIterator<IndexedFile> getFileChangesWithRateLimit(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSource.AdmissionLimits> limits) {
    // TODO(#5319): getFileChangesForCDC if CDC is enabled.

    CloseableIterator<IndexedFile> changes =
        getFileChanges(
            fromVersion, fromIndex, isInitialSnapshot, /* endOffset= */ Optional.empty());

    // Take each change until we've seen the configured number of addFiles. Some changes don't
    // represent file additions; we retain them for offset tracking, but they don't count toward
    // the maxFilesPerTrigger conf.
    if (limits.isPresent()) {
      DeltaSource.AdmissionLimits admissionLimits = limits.get();
      changes = changes.takeWhile(admissionLimits::admit);
    }

    // TODO(#5318): Stop at schema change barriers
    return changes;
  }

  /**
   * Get file changes between fromVersion/fromIndex and endOffset. This is the Kernel-based
   * implementation of DeltaSource.getFileChanges.
   *
   * <p>Package-private for testing.
   *
   * @param fromVersion The starting version (exclusive with fromIndex)
   * @param fromIndex The starting index within fromVersion (exclusive)
   * @param isInitialSnapshot Whether this is the initial snapshot
   * @param endOffset The end offset (inclusive), or empty to read all available commits
   * @return An iterator of IndexedFile representing the file changes
   */
  CloseableIterator<IndexedFile> getFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSourceOffset> endOffset) {

    CloseableIterator<IndexedFile> result;

    if (isInitialSnapshot) {
      // TODO(#5318): Implement initial snapshot
      throw new UnsupportedOperationException("initial snapshot is not supported yet");
    } else {
      result = filterDeltaLogs(fromVersion, endOffset);
    }

    // Check start boundary (exclusive)
    result =
        result.filter(
            file ->
                file.getVersion() > fromVersion
                    || (file.getVersion() == fromVersion && file.getIndex() > fromIndex));

    Optional<DeltaSourceOffset> lastOffsetForThisScan =
        endOffset.or(() -> lastOffsetForTriggerAvailableNow);
    // Check end boundary (inclusive)
    if (lastOffsetForThisScan.isPresent()) {
      DeltaSourceOffset bound = lastOffsetForThisScan.get();
      result =
          result.takeWhile(
              file ->
                  file.getVersion() < bound.reservoirVersion()
                      || (file.getVersion() == bound.reservoirVersion()
                          && file.getIndex() <= bound.index()));
    }

    return result;
  }

  // TODO(#5318): implement lazy loading (one batch at a time).
  private CloseableIterator<IndexedFile> filterDeltaLogs(
      long startVersion, Optional<DeltaSourceOffset> endOffset) {
    List<IndexedFile> allIndexedFiles = new ArrayList<>();
    Optional<Long> endVersionOpt =
        endOffset.isPresent() ? Optional.of(endOffset.get().reservoirVersion()) : Optional.empty();

    CommitRange commitRange;
    try {
      commitRange = snapshotManager.getTableChanges(engine, startVersion, endVersionOpt);
    } catch (io.delta.kernel.exceptions.CommitRangeNotFoundException e) {
      // If the requested version range doesn't exist (e.g., we're asking for version 6 when
      // the table only has versions 0-5).
      return Utils.toCloseableIterator(allIndexedFiles.iterator());
    }

    // Required by kernel: perform protocol validation by creating a snapshot at startVersion.
    Snapshot startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
    String tablePath = startSnapshot.getPath();
    try (CloseableIterator<ColumnarBatch> actionsIter =
        commitRange.getActions(engine, startSnapshot, ACTION_SET)) {
      // Each ColumnarBatch belongs to a single commit version,
      // but a single version may span multiple ColumnarBatches.
      long currentVersion = -1;
      long currentIndex = 0;
      List<IndexedFile> currentVersionFiles = new ArrayList<>();

      while (actionsIter.hasNext()) {
        ColumnarBatch batch = actionsIter.next();
        if (batch.getSize() == 0) {
          // TODO(#5318): this shouldn't happen, empty commits will still have a non-empty row
          // with the version set. Make sure the kernel API is explicit about this.
          continue;
        }
        long version = StreamingHelper.getVersion(batch);
        // When version changes, flush the completed version
        if (currentVersion != -1 && version != currentVersion) {
          flushVersion(currentVersion, currentVersionFiles, allIndexedFiles);
          currentVersionFiles.clear();
          currentIndex = 0;
        }

        // Validate the commit before processing files from this batch
        // TODO(#5318): migrate to kernel's commit-level iterator (WIP).
        // The current one-pass algorithm assumes REMOVE actions proceed ADD actions
        // in a commit; we should implement a proper two-pass approach once kernel API is ready.
        validateCommit(batch, version, startVersion, tablePath, endOffset);

        currentVersion = version;
        currentIndex =
            extractIndexedFilesFromBatch(batch, version, currentIndex, currentVersionFiles);
      }

      // Flush the last version
      if (currentVersion != -1) {
        flushVersion(currentVersion, currentVersionFiles, allIndexedFiles);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read commit range", e);
    }
    // TODO(#5318): implement lazy loading (only load a batch into memory if needed).
    return Utils.toCloseableIterator(allIndexedFiles.iterator());
  }

  /**
   * Flushes a completed version by adding BEGIN/END sentinels around data files.
   *
   * <p>Sentinels are IndexedFiles with null addFile that mark version boundaries. They serve
   * several purposes:
   *
   * <ul>
   *   <li>Enable offset tracking at version boundaries (before any files or after all files)
   *   <li>Allow streaming to resume at the start or end of a version
   *   <li>Handle versions with only metadata/protocol changes (no data files)
   * </ul>
   *
   * <p>This mimics DeltaSource.addBeginAndEndIndexOffsetsForVersion
   */
  private void flushVersion(
      long version, List<IndexedFile> versionFiles, List<IndexedFile> output) {
    // Add BEGIN sentinel
    output.add(new IndexedFile(version, DeltaSourceOffset.BASE_INDEX(), /* addFile= */ null));
    // TODO(#5319): implement getMetadataOrProtocolChangeIndexedFileIterator.
    // Add all data files
    output.addAll(versionFiles);
    // Add END sentinel
    output.add(new IndexedFile(version, DeltaSourceOffset.END_INDEX(), /* addFile= */ null));
  }

  /**
   * Validates a commit and fail the stream if it's invalid. Mimics
   * DeltaSource.validateCommitAndDecideSkipping in Scala.
   *
   * @throws RuntimeException if the commit is invalid.
   */
  private void validateCommit(
      ColumnarBatch batch,
      long version,
      long startVersion,
      String tablePath,
      Optional<DeltaSourceOffset> endOffsetOpt) {
    // If endOffset is at the beginning of this version, exit early.
    if (endOffsetOpt.isPresent()) {
      DeltaSourceOffset endOffset = endOffsetOpt.get();
      if (endOffset.reservoirVersion() == version
          && endOffset.index() == DeltaSourceOffset.BASE_INDEX()) {
        return;
      }
    }
    int numRows = batch.getSize();
    // TODO(#5319): Implement ignoreChanges & skipChangeCommits & ignoreDeletes (legacy)
    // TODO(#5318): validate METADATA actions
    Metadata metadataAction = null;
    for (int rowId = 0; rowId < numRows; rowId++) {
      // RULE 1: If commit has RemoveFile(dataChange=true), fail this stream.
      Optional<RemoveFile> removeOpt = StreamingHelper.getDataChangeRemove(batch, rowId);
      if (removeOpt.isPresent()) {
        RemoveFile removeFile = removeOpt.get();
        throw (RuntimeException)
            DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFile.getPath(), tablePath);
      }

      // RULE 2: If commit has Metadata, check read-incompatible schema changes.
      Optional<Metadata> metadataOpt = StreamingHelper.getMetadata(batch, rowId);
      if (metadataOpt.isPresent()) {
        Metadata metadata = metadataOpt.get();
        checkReadIncompatibleSchemaChanges(metadata, version, startVersion, false);
        Preconditions.checkArgument(metadataAction == null, "Should not encounter two metadata actions in the same commit");
        metadataAction = metadata;
      }
    }
  }

  protected void checkReadIncompatibleSchemaChanges(
      Metadata metadata,
      long version,
      long startVersion,
      boolean validatedDuringStreamStart) {
    Metadata newMetadata, oldMetadata;
    if (version < snapshotAtSourceInit.getVersion()) {
      newMetadata = metadataAtSourceInit;
      oldMetadata = metadata;
    } else {
      newMetadata = metadata;
      oldMetadata = metadataAtSourceInit;
    }

    // Table ID has changed during streaming
    if (!Objects.equals(newMetadata.getId(), oldMetadata.getId())) {
      throw (RuntimeException)
          DeltaErrors.differentDeltaTableReadByStreamingSource(newMetadata.getId(), oldMetadata.getId());
    }

    org.apache.spark.sql.types.StructType newKernelSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(newMetadata.getSchema());
    org.apache.spark.sql.types.StructType oldKernelSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(oldMetadata.getSchema());
    boolean shouldTrackSchema;
    if (typeWideningEnabled && enableSchemaTrackingForTypeWidening &&
        TypeWidening.containsWideningTypeChanges(oldKernelSchema, newKernelSchema)) {
      // If schema tracking is enabled for type widening, we will detect widening type changes and
      // block the stream until the user sets `allowSourceColumnTypeChange` - similar to handling
      // DROP/RENAME for column mapping.
      shouldTrackSchema = true;
    } else if (allowUnsafeStreamingReadOnColumnMappingSchemaChanges) {
      shouldTrackSchema = false;
    } else {
      // TODO: Column mapping schema changes
      shouldTrackSchema = true;
    }

    if (shouldTrackSchema) {
      throw (RuntimeException) DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
          spark,
          SchemaUtils.convertKernelSchemaToSparkSchema(oldMetadata.getSchema()),
          SchemaUtils.convertKernelSchemaToSparkSchema(newMetadata.getSchema()),
          !validatedDuringStreamStart);
    }

    // Other standard read compatibility changes
    if (!validatedDuringStreamStart ||
        !forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart) {

      // TODO: CDC support
      StructType schemaChange = metadata.getSchema();
      org.apache.spark.sql.types.StructType sparkSchemaChange =
          SchemaUtils.convertKernelSchemaToSparkSchema(schemaChange);
      org.apache.spark.sql.types.StructType readSparkSchemaAtSourceInit =
          SchemaUtils.convertKernelSchemaToSparkSchema(readSchemaAtSourceInit);

      // There is a schema change. All the files after this commit will use `schemaChange`. Hence,
      // we check whether we can use `schema` (the fixed source schema we use in the same run of the
      // query) to read these new files safely.
      boolean backfilling = version < snapshotAtSourceInit.getVersion();
      // We forbid the case when the schemaChange is nullable while the read schema is NOT
      // nullable, or in other words, `schema` should not tighten nullability from `schemaChange`,
      // because we don't ever want to read back any nulls when the read schema is non-nullable.
      boolean shouldForbidTightenNullability = !forceEnableUnsafeReadOnNullabilityChange;
      // If schema tracking is disabled for type widening, we allow widening type changes to go
      // through without requiring the user to set `allowSourceColumnTypeChange`. The schema change
      // will cause the stream to fail with a retryable exception, and the stream will restart using
      // the new schema.
      TypeWideningMode typeWideningMode =
          typeWideningEnabled && !enableSchemaTrackingForTypeWidening
              ? TypeWideningMode.AllTypeWidening$.MODULE$
              : TypeWideningMode.NoTypeWidening$.MODULE$;
      boolean allowTypeWidening = typeWideningEnabled && !enableSchemaTrackingForTypeWidening;
      Seq<String> newPartitionColumnsSeq = CollectionConverters.asScala(
          VectorUtils.toJavaList(newMetadata.getPartitionColumns()).stream()
              .map(Object::toString)
              .collect(Collectors.toList())
      ).toSeq();
      Seq<String> oldPartitionColumnsSeq = CollectionConverters.asScala(
          VectorUtils.toJavaList(oldMetadata.getPartitionColumns()).stream()
              .map(Object::toString)
              .collect(Collectors.toList())
      ).toSeq();

      if (org.apache.spark.sql.delta.schema.SchemaUtils.isReadCompatible(
          sparkSchemaChange, readSparkSchemaAtSourceInit,
          shouldForbidTightenNullability,
          // If a user is streaming from a column mapping table and enable the unsafe flag to ignore
          // column mapping schema changes, we can allow the standard check to allow missing columns
          // from the read schema in the schema change, because the only case that happens is when
          // user rename/drops column, but they don't care so they enabled the flag to unblock.
          // This is only allowed when we are "backfilling", i.e. the stream progress is older than
          // the analyzed table version. Any schema change past the analysis should still throw
          // exception, because additive schema changes MUST be taken into account.
          isStreamingFromColumnMappingTable &&
              allowUnsafeStreamingReadOnColumnMappingSchemaChanges &&
              backfilling,
          typeWideningMode,
          allowUnsafeStreamingReadOnPartitionColumnChanges
              ? (Seq<String>) Seq$.MODULE$.empty() : newPartitionColumnsSeq,
          allowUnsafeStreamingReadOnPartitionColumnChanges
              ? (Seq<String>) Seq$.MODULE$.empty() : oldPartitionColumnsSeq
      )) {
        // Only schema change later than the current read snapshot/schema can be retried, in other
        // words, backfills could never be retryable, because we have no way to refresh
        // the latest schema to "catch up" when the schema change happens before than current read
        // schema version.
        // If not backfilling, we do another check to determine retryability, in which we assume
        // we will be reading using this later `schemaChange` back on the current outdated `schema`,
        // and if it works (including that `schemaChange` should not tighten the nullability
        // constraint from `schema`), it is a retryable exception.
        boolean retryable = !backfilling && org.apache.spark.sql.delta.schema.SchemaUtils.isReadCompatible(
            readSparkSchemaAtSourceInit,
            sparkSchemaChange,
            shouldForbidTightenNullability,
            false,
            typeWideningMode,
            (Seq<String>) Seq$.MODULE$.empty(),
            (Seq<String>) Seq$.MODULE$.empty()
        );
        throw (RuntimeException) DeltaErrors.schemaChangedException(
            readSparkSchemaAtSourceInit,
            sparkSchemaChange,
            retryable,
            Option.apply(version),
            options.containsStartingVersionOrTimestamp()
        );
      }
    }
  }

  /**
   * Extracts IndexedFiles from a batch of actions for a given version and adds them to the output
   * list. Assigns an index to each IndexedFile.
   *
   * @return The next available index after processing this batch
   */
  private long extractIndexedFilesFromBatch(
      ColumnarBatch batch, long version, long startIndex, List<IndexedFile> output) {
    long index = startIndex;
    for (int rowId = 0; rowId < batch.getSize(); rowId++) {
      Optional<AddFile> addOpt = StreamingHelper.getDataChangeAdd(batch, rowId);
      if (addOpt.isPresent()) {
        AddFile addFile = addOpt.get();
        output.add(new IndexedFile(version, index++, addFile));
      }
    }

    return index;
  }
}
