// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import org.slf4j.LoggerFactory;
import java.net.URI;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.google.common.annotations.VisibleForTesting;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import java.util.Date;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import java.io.InterruptedIOException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import java.util.Arrays;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import org.apache.hadoop.fs.permission.FsPermission;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.Tristate;
import java.util.Map;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.AmazonClientException;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.util.ReflectionUtils;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.conf.Configuration;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DynamoDBMetadataStore implements MetadataStore
{
    public static final Logger LOG;
    public static final String VERSION_MARKER = "../VERSION";
    public static final int VERSION = 100;
    public static final String E_NO_VERSION_MARKER = "S3Guard table lacks version marker.";
    public static final String E_INCOMPATIBLE_VERSION = "Database table is from an incompatible S3Guard version.";
    public static final long MIN_RETRY_SLEEP_MSEC = 100L;
    private static ValueMap deleteTrackingValueMap;
    private DynamoDB dynamoDB;
    private String region;
    private Table table;
    private String tableName;
    private Configuration conf;
    private String username;
    private RetryPolicy dataAccessRetryPolicy;
    private S3AInstrumentation.S3GuardInstrumentation instrumentation;
    
    private static DynamoDB createDynamoDB(final Configuration conf, final String s3Region) throws IOException {
        Preconditions.checkNotNull((Object)conf);
        final Class<? extends DynamoDBClientFactory> cls = (Class<? extends DynamoDBClientFactory>)conf.getClass("fs.s3a.s3guard.ddb.client.factory.impl", (Class)S3Guard.S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT, (Class)DynamoDBClientFactory.class);
        DynamoDBMetadataStore.LOG.debug("Creating DynamoDB client {} with S3 region {}", (Object)cls, (Object)s3Region);
        final AmazonDynamoDB dynamoDBClient = ((DynamoDBClientFactory)ReflectionUtils.newInstance((Class)cls, conf)).createDynamoDBClient(s3Region);
        return new DynamoDB(dynamoDBClient);
    }
    
    @Override
    public void initialize(final FileSystem fs) throws IOException {
        Preconditions.checkArgument(fs instanceof S3AFileSystem, (Object)"DynamoDBMetadataStore only supports S3A filesystem.");
        final S3AFileSystem s3afs = (S3AFileSystem)fs;
        this.instrumentation = s3afs.getInstrumentation().getS3GuardInstrumentation();
        final String bucket = s3afs.getBucket();
        final String confRegion = s3afs.getConf().getTrimmed("fs.s3a.s3guard.ddb.region");
        if (!StringUtils.isEmpty(confRegion)) {
            this.region = confRegion;
            DynamoDBMetadataStore.LOG.debug("Overriding S3 region with configured DynamoDB region: {}", (Object)this.region);
        }
        else {
            this.region = s3afs.getBucketLocation();
            DynamoDBMetadataStore.LOG.debug("Inferring DynamoDB region from S3 bucket: {}", (Object)this.region);
        }
        this.username = s3afs.getUsername();
        this.conf = s3afs.getConf();
        this.dynamoDB = createDynamoDB(this.conf, this.region);
        this.tableName = this.conf.getTrimmed("fs.s3a.s3guard.ddb.table", bucket);
        this.setMaxRetries(this.conf);
        this.initTable();
        this.instrumentation.initialized();
    }
    
    @Override
    public void initialize(final Configuration config) throws IOException {
        this.conf = config;
        this.tableName = this.conf.getTrimmed("fs.s3a.s3guard.ddb.table");
        Preconditions.checkArgument(!StringUtils.isEmpty(this.tableName), (Object)"No DynamoDB table name configured!");
        this.region = this.conf.getTrimmed("fs.s3a.s3guard.ddb.region");
        Preconditions.checkArgument(!StringUtils.isEmpty(this.region), (Object)"No DynamoDB region configured!");
        this.dynamoDB = createDynamoDB(this.conf, this.region);
        this.username = UserGroupInformation.getCurrentUser().getShortUserName();
        this.setMaxRetries(this.conf);
        this.initTable();
    }
    
    private void setMaxRetries(final Configuration config) {
        final int maxRetries = config.getInt("fs.s3a.s3guard.ddb.max.retries", 9);
        this.dataAccessRetryPolicy = RetryPolicies.exponentialBackoffRetry(maxRetries, 100L, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void delete(final Path path) throws IOException {
        this.innerDelete(path, true);
    }
    
    @Override
    public void forgetMetadata(final Path path) throws IOException {
        this.innerDelete(path, false);
    }
    
    private void innerDelete(Path path, final boolean tombstone) throws IOException {
        path = this.checkPath(path);
        DynamoDBMetadataStore.LOG.debug("Deleting from table {} in region {}: {}", new Object[] { this.tableName, this.region, path });
        if (path.isRoot()) {
            DynamoDBMetadataStore.LOG.debug("Skip deleting root directory as it does not exist in table");
            return;
        }
        try {
            if (tombstone) {
                final Item item = PathMetadataDynamoDBTranslation.pathMetadataToItem(PathMetadata.tombstone(path));
                this.table.putItem(item);
            }
            else {
                this.table.deleteItem(PathMetadataDynamoDBTranslation.pathToKey(path));
            }
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("delete", path, e);
        }
    }
    
    @Override
    public void deleteSubtree(Path path) throws IOException {
        path = this.checkPath(path);
        DynamoDBMetadataStore.LOG.debug("Deleting subtree from table {} in region {}: {}", new Object[] { this.tableName, this.region, path });
        final PathMetadata meta = this.get(path);
        if (meta == null || meta.isDeleted()) {
            DynamoDBMetadataStore.LOG.debug("Subtree path {} does not exist; this will be a no-op", (Object)path);
            return;
        }
        final DescendantsIterator desc = new DescendantsIterator(this, meta);
        while (desc.hasNext()) {
            this.innerDelete(desc.next().getPath(), true);
        }
    }
    
    private Item getConsistentItem(final PrimaryKey key) {
        final GetItemSpec spec = new GetItemSpec().withPrimaryKey(key).withConsistentRead(true);
        return this.table.getItem(spec);
    }
    
    @Override
    public PathMetadata get(final Path path) throws IOException {
        return this.get(path, false);
    }
    
    @Override
    public PathMetadata get(Path path, final boolean wantEmptyDirectoryFlag) throws IOException {
        path = this.checkPath(path);
        DynamoDBMetadataStore.LOG.debug("Get from table {} in region {}: {}", new Object[] { this.tableName, this.region, path });
        try {
            PathMetadata meta;
            if (path.isRoot()) {
                meta = new PathMetadata(this.makeDirStatus(this.username, path));
            }
            else {
                final Item item = this.getConsistentItem(PathMetadataDynamoDBTranslation.pathToKey(path));
                meta = PathMetadataDynamoDBTranslation.itemToPathMetadata(item, this.username);
                DynamoDBMetadataStore.LOG.debug("Get from table {} in region {} returning for {}: {}", new Object[] { this.tableName, this.region, path, meta });
            }
            if (wantEmptyDirectoryFlag && meta != null) {
                final FileStatus status = meta.getFileStatus();
                if (status.isDirectory()) {
                    final QuerySpec spec = new QuerySpec().withHashKey(PathMetadataDynamoDBTranslation.pathToParentKeyAttribute(path)).withConsistentRead(true).withFilterExpression("is_deleted = :false").withValueMap((Map)DynamoDBMetadataStore.deleteTrackingValueMap);
                    final ItemCollection<QueryOutcome> items = (ItemCollection<QueryOutcome>)this.table.query(spec);
                    final boolean hasChildren = items.iterator().hasNext();
                    meta.setIsEmptyDirectory(hasChildren ? Tristate.FALSE : Tristate.UNKNOWN);
                }
            }
            return meta;
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("get", path, e);
        }
    }
    
    private FileStatus makeDirStatus(final String owner, final Path path) {
        return new FileStatus(0L, true, 1, 0L, 0L, 0L, (FsPermission)null, owner, (String)null, path);
    }
    
    @Override
    public DirListingMetadata listChildren(Path path) throws IOException {
        path = this.checkPath(path);
        DynamoDBMetadataStore.LOG.debug("Listing table {} in region {}: {}", new Object[] { this.tableName, this.region, path });
        try {
            final QuerySpec spec = new QuerySpec().withHashKey(PathMetadataDynamoDBTranslation.pathToParentKeyAttribute(path)).withConsistentRead(true);
            final ItemCollection<QueryOutcome> items = (ItemCollection<QueryOutcome>)this.table.query(spec);
            final List<PathMetadata> metas = new ArrayList<PathMetadata>();
            for (final Item item : items) {
                final PathMetadata meta = PathMetadataDynamoDBTranslation.itemToPathMetadata(item, this.username);
                metas.add(meta);
            }
            DynamoDBMetadataStore.LOG.trace("Listing table {} in region {} for {} returning {}", new Object[] { this.tableName, this.region, path, metas });
            return (metas.isEmpty() && this.get(path) == null) ? null : new DirListingMetadata(path, metas, false);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("listChildren", path, e);
        }
    }
    
    @Override
    public void move(final Collection<Path> pathsToDelete, final Collection<PathMetadata> pathsToCreate) throws IOException {
        if (pathsToDelete == null && pathsToCreate == null) {
            return;
        }
        DynamoDBMetadataStore.LOG.debug("Moving paths of table {} in region {}: {} paths to delete and {} paths to create", new Object[] { this.tableName, this.region, (pathsToDelete == null) ? 0 : pathsToDelete.size(), (pathsToCreate == null) ? 0 : pathsToCreate.size() });
        DynamoDBMetadataStore.LOG.trace("move: pathsToDelete = {}, pathsToCreate = {}", (Object)pathsToDelete, (Object)pathsToCreate);
        final Collection<PathMetadata> newItems = new ArrayList<PathMetadata>();
        if (pathsToCreate != null) {
            newItems.addAll(pathsToCreate);
            final Collection<Path> fullPathsToCreate = new HashSet<Path>();
            for (final PathMetadata meta : pathsToCreate) {
                fullPathsToCreate.add(meta.getFileStatus().getPath());
            }
            for (final PathMetadata meta : pathsToCreate) {
                Preconditions.checkArgument(meta != null);
                for (Path parent = meta.getFileStatus().getPath().getParent(); parent != null && !parent.isRoot() && !fullPathsToCreate.contains(parent); parent = parent.getParent()) {
                    DynamoDBMetadataStore.LOG.debug("move: auto-create ancestor path {} for child path {}", (Object)parent, (Object)meta.getFileStatus().getPath());
                    final FileStatus status = makeDirStatus(parent, this.username);
                    newItems.add(new PathMetadata(status, Tristate.FALSE, false));
                    fullPathsToCreate.add(parent);
                }
            }
        }
        if (pathsToDelete != null) {
            for (final Path meta2 : pathsToDelete) {
                newItems.add(PathMetadata.tombstone(meta2));
            }
        }
        try {
            this.processBatchWriteRequest(null, PathMetadataDynamoDBTranslation.pathMetadataToItem(newItems));
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("move", (String)null, e);
        }
    }
    
    private void processBatchWriteRequest(final PrimaryKey[] keysToDelete, final Item[] itemsToPut) throws IOException {
        final int totalToDelete = (keysToDelete == null) ? 0 : keysToDelete.length;
        final int totalToPut = (itemsToPut == null) ? 0 : itemsToPut.length;
        int count = 0;
        while (count < totalToDelete + totalToPut) {
            final TableWriteItems writeItems = new TableWriteItems(this.tableName);
            int numToDelete = 0;
            if (keysToDelete != null && count < totalToDelete) {
                numToDelete = Math.min(25, totalToDelete - count);
                writeItems.withPrimaryKeysToDelete((PrimaryKey[])Arrays.copyOfRange(keysToDelete, count, count + numToDelete));
                count += numToDelete;
            }
            if (numToDelete < 25 && itemsToPut != null && count < totalToDelete + totalToPut) {
                final int numToPut = Math.min(25 - numToDelete, totalToDelete + totalToPut - count);
                final int index = count - totalToDelete;
                writeItems.withItemsToPut((Item[])Arrays.copyOfRange(itemsToPut, index, index + numToPut));
                count += numToPut;
            }
            BatchWriteItemOutcome res = this.dynamoDB.batchWriteItem(new TableWriteItems[] { writeItems });
            Map<String, List<WriteRequest>> unprocessed = (Map<String, List<WriteRequest>>)res.getUnprocessedItems();
            int retryCount = 0;
            while (unprocessed.size() > 0) {
                this.retryBackoff(retryCount++);
                res = this.dynamoDB.batchWriteItemUnprocessed((Map)unprocessed);
                unprocessed = (Map<String, List<WriteRequest>>)res.getUnprocessedItems();
            }
        }
    }
    
    private void retryBackoff(final int retryCount) throws IOException {
        try {
            final RetryPolicy.RetryAction action = this.dataAccessRetryPolicy.shouldRetry((Exception)null, retryCount, 0, true);
            if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                throw new IOException(String.format("Max retries exceeded (%d) for DynamoDB", retryCount));
            }
            DynamoDBMetadataStore.LOG.debug("Sleeping {} msec before next retry", (Object)action.delayMillis);
            Thread.sleep(action.delayMillis);
        }
        catch (Exception e) {
            throw new IOException("Unexpected exception", e);
        }
    }
    
    @Override
    public void put(final PathMetadata meta) throws IOException {
        DynamoDBMetadataStore.LOG.debug("Saving to table {} in region {}: {}", new Object[] { this.tableName, this.region, meta });
        this.processBatchWriteRequest(null, PathMetadataDynamoDBTranslation.pathMetadataToItem(this.fullPathsToPut(meta)));
    }
    
    private Collection<PathMetadata> fullPathsToPut(final PathMetadata meta) throws IOException {
        checkPathMetadata(meta);
        final Collection<PathMetadata> metasToPut = new ArrayList<PathMetadata>();
        if (!meta.getFileStatus().getPath().isRoot()) {
            metasToPut.add(meta);
        }
        for (Path path = meta.getFileStatus().getPath().getParent(); path != null && !path.isRoot(); path = path.getParent()) {
            final Item item = this.getConsistentItem(PathMetadataDynamoDBTranslation.pathToKey(path));
            if (this.itemExists(item)) {
                break;
            }
            final FileStatus status = makeDirStatus(path, this.username);
            metasToPut.add(new PathMetadata(status, Tristate.FALSE, false));
        }
        return metasToPut;
    }
    
    private boolean itemExists(final Item item) {
        return item != null && (!item.hasAttribute("is_deleted") || !item.getBoolean("is_deleted"));
    }
    
    static FileStatus makeDirStatus(final Path f, final String owner) {
        return new FileStatus(0L, true, 1, 0L, System.currentTimeMillis(), 0L, (FsPermission)null, owner, owner, f);
    }
    
    @Override
    public void put(final DirListingMetadata meta) throws IOException {
        DynamoDBMetadataStore.LOG.debug("Saving to table {} in region {}: {}", new Object[] { this.tableName, this.region, meta });
        final PathMetadata p = new PathMetadata(makeDirStatus(meta.getPath(), this.username), meta.isEmpty(), false);
        final Collection<PathMetadata> metasToPut = this.fullPathsToPut(p);
        metasToPut.addAll(meta.getListing());
        try {
            this.processBatchWriteRequest(null, PathMetadataDynamoDBTranslation.pathMetadataToItem(metasToPut));
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("put", (String)null, e);
        }
    }
    
    @Override
    public synchronized void close() {
        if (this.instrumentation != null) {
            this.instrumentation.storeClosed();
        }
        if (this.dynamoDB != null) {
            DynamoDBMetadataStore.LOG.debug("Shutting down {}", (Object)this);
            this.dynamoDB.shutdown();
            this.dynamoDB = null;
        }
    }
    
    @Override
    public void destroy() throws IOException {
        if (this.table == null) {
            DynamoDBMetadataStore.LOG.info("In destroy(): no table to delete");
            return;
        }
        DynamoDBMetadataStore.LOG.info("Deleting DynamoDB table {} in region {}", (Object)this.tableName, (Object)this.region);
        Preconditions.checkNotNull((Object)this.dynamoDB, (Object)"Not connected to Dynamo");
        try {
            this.table.delete();
            this.table.waitForDelete();
        }
        catch (ResourceNotFoundException rnfe) {
            DynamoDBMetadataStore.LOG.info("ResourceNotFoundException while deleting DynamoDB table {} in region {}.  This may indicate that the table does not exist, or has been deleted by another concurrent thread or process.", (Object)this.tableName, (Object)this.region);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            DynamoDBMetadataStore.LOG.warn("Interrupted while waiting for DynamoDB table {} being deleted", (Object)this.tableName, (Object)ie);
            throw new InterruptedIOException("Table " + this.tableName + " in region " + this.region + " has not been deleted");
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("destroy", (String)null, e);
        }
    }
    
    private ItemCollection<ScanOutcome> expiredFiles(final long modTime) {
        final String filterExpression = "mod_time < :mod_time";
        final String projectionExpression = "parent,child";
        final ValueMap map = new ValueMap().withLong(":mod_time", modTime);
        return (ItemCollection<ScanOutcome>)this.table.scan(filterExpression, projectionExpression, (Map)null, (Map)map);
    }
    
    @Override
    public void prune(final long modTime) throws IOException {
        int itemCount = 0;
        try {
            final Collection<Path> deletionBatch = new ArrayList<Path>(25);
            final int delay = this.conf.getInt("fs.s3a.s3guard.ddb.background.sleep", 25);
            for (final Item item : this.expiredFiles(modTime)) {
                final PathMetadata md = PathMetadataDynamoDBTranslation.itemToPathMetadata(item, this.username);
                final Path path = md.getFileStatus().getPath();
                deletionBatch.add(path);
                ++itemCount;
                if (deletionBatch.size() == 25) {
                    Thread.sleep(delay);
                    this.processBatchWriteRequest(PathMetadataDynamoDBTranslation.pathToKey(deletionBatch), null);
                    deletionBatch.clear();
                }
            }
            if (deletionBatch.size() > 0) {
                Thread.sleep(delay);
                this.processBatchWriteRequest(PathMetadataDynamoDBTranslation.pathToKey(deletionBatch), null);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Pruning was interrupted");
        }
        DynamoDBMetadataStore.LOG.info("Finished pruning {} items in batches of {}", (Object)itemCount, (Object)25);
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + '{' + "region=" + this.region + ", tableName=" + this.tableName + '}';
    }
    
    @VisibleForTesting
    void initTable() throws IOException {
        this.table = this.dynamoDB.getTable(this.tableName);
        try {
            try {
                DynamoDBMetadataStore.LOG.debug("Binding to table {}", (Object)this.tableName);
                final String tableStatus;
                final String status = tableStatus = this.table.describe().getTableStatus();
                switch (tableStatus) {
                    case "CREATING":
                    case "UPDATING": {
                        DynamoDBMetadataStore.LOG.debug("Table {} in region {} is being created/updated. This may indicate that the table is being operated by another concurrent thread or process. Waiting for active...", (Object)this.tableName, (Object)this.region);
                        this.waitForTableActive(this.table);
                        break;
                    }
                    case "DELETING": {
                        throw new IOException("DynamoDB table '" + this.tableName + "' is being " + "deleted in region " + this.region);
                    }
                    case "ACTIVE": {
                        break;
                    }
                    default: {
                        throw new IOException("Unknown DynamoDB table status " + status + ": tableName='" + this.tableName + "', region=" + this.region);
                    }
                }
                final Item versionMarker = this.getVersionMarkerItem();
                verifyVersionCompatibility(this.tableName, versionMarker);
                final Long created = PathMetadataDynamoDBTranslation.extractCreationTimeFromMarker(versionMarker);
                DynamoDBMetadataStore.LOG.debug("Using existing DynamoDB table {} in region {} created {}", new Object[] { this.tableName, this.region, (created != null) ? new Date(created) : null });
            }
            catch (ResourceNotFoundException rnfe) {
                if (!this.conf.getBoolean("fs.s3a.s3guard.ddb.table.create", false)) {
                    throw new IOException("DynamoDB table '" + this.tableName + "' does not " + "exist in region " + this.region + "; auto-creation is turned off");
                }
                final ProvisionedThroughput capacity = new ProvisionedThroughput(Long.valueOf(this.conf.getLong("fs.s3a.s3guard.ddb.table.capacity.read", 500L)), Long.valueOf(this.conf.getLong("fs.s3a.s3guard.ddb.table.capacity.write", 100L)));
                this.createTable(capacity);
            }
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("initTable", (String)null, e);
        }
    }
    
    private Item getVersionMarkerItem() throws IOException {
        final PrimaryKey versionMarkerKey = PathMetadataDynamoDBTranslation.createVersionMarkerPrimaryKey("../VERSION");
        int retryCount = 0;
        Item versionMarker;
        for (versionMarker = this.table.getItem(versionMarkerKey); versionMarker == null; versionMarker = this.table.getItem(versionMarkerKey)) {
            try {
                final RetryPolicy.RetryAction action = this.dataAccessRetryPolicy.shouldRetry((Exception)null, retryCount, 0, true);
                if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                    break;
                }
                DynamoDBMetadataStore.LOG.debug("Sleeping {} ms before next retry", (Object)action.delayMillis);
                Thread.sleep(action.delayMillis);
            }
            catch (Exception e) {
                throw new IOException("initTable: Unexpected exception", e);
            }
            ++retryCount;
        }
        return versionMarker;
    }
    
    @VisibleForTesting
    static void verifyVersionCompatibility(final String tableName, final Item versionMarker) throws IOException {
        if (versionMarker == null) {
            DynamoDBMetadataStore.LOG.warn("Table {} contains no version marker", (Object)tableName);
            throw new IOException("S3Guard table lacks version marker. Table: " + tableName);
        }
        final int version = PathMetadataDynamoDBTranslation.extractVersionFromMarker(versionMarker);
        if (100 != version) {
            throw new IOException("Database table is from an incompatible S3Guard version. Table " + tableName + " Expected version " + 100 + " actual " + version);
        }
    }
    
    private void waitForTableActive(final Table table) throws IOException {
        try {
            table.waitForActive();
        }
        catch (InterruptedException e) {
            DynamoDBMetadataStore.LOG.warn("Interrupted while waiting for table {} in region {} active", new Object[] { this.tableName, this.region, e });
            Thread.currentThread().interrupt();
            throw (IOException)new InterruptedIOException("DynamoDB table '" + this.tableName + "' is not active yet in region " + this.region).initCause(e);
        }
    }
    
    private void createTable(final ProvisionedThroughput capacity) throws IOException {
        try {
            DynamoDBMetadataStore.LOG.info("Creating non-existent DynamoDB table {} in region {}", (Object)this.tableName, (Object)this.region);
            this.table = this.dynamoDB.createTable(new CreateTableRequest().withTableName(this.tableName).withKeySchema((Collection)PathMetadataDynamoDBTranslation.keySchema()).withAttributeDefinitions((Collection)PathMetadataDynamoDBTranslation.attributeDefinitions()).withProvisionedThroughput(capacity));
            DynamoDBMetadataStore.LOG.debug("Awaiting table becoming active");
        }
        catch (ResourceInUseException e) {
            DynamoDBMetadataStore.LOG.warn("ResourceInUseException while creating DynamoDB table {} in region {}.  This may indicate that the table was created by another concurrent thread or process.", (Object)this.tableName, (Object)this.region);
        }
        this.waitForTableActive(this.table);
        final Item marker = PathMetadataDynamoDBTranslation.createVersionMarker("../VERSION", 100, System.currentTimeMillis());
        this.putItem(marker);
    }
    
    PutItemOutcome putItem(final Item item) {
        DynamoDBMetadataStore.LOG.debug("Putting item {}", (Object)item);
        return this.table.putItem(item);
    }
    
    void provisionTable(final Long readCapacity, final Long writeCapacity) throws IOException {
        final ProvisionedThroughput toProvision = new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity);
        try {
            final ProvisionedThroughputDescription p = this.table.updateTable(toProvision).getProvisionedThroughput();
            DynamoDBMetadataStore.LOG.info("Provision table {} in region {}: readCapacityUnits={}, writeCapacityUnits={}", new Object[] { this.tableName, this.region, p.getReadCapacityUnits(), p.getWriteCapacityUnits() });
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("provisionTable", (String)null, e);
        }
    }
    
    Table getTable() {
        return this.table;
    }
    
    String getRegion() {
        return this.region;
    }
    
    @VisibleForTesting
    DynamoDB getDynamoDB() {
        return this.dynamoDB;
    }
    
    private Path checkPath(final Path path) {
        Preconditions.checkNotNull((Object)path);
        Preconditions.checkArgument(path.isAbsolute(), "Path %s is not absolute", new Object[] { path });
        final URI uri = path.toUri();
        Preconditions.checkNotNull((Object)uri.getScheme(), "Path %s missing scheme", new Object[] { path });
        Preconditions.checkArgument(uri.getScheme().equals("s3a"), "Path %s scheme must be %s", new Object[] { path, "s3a" });
        Preconditions.checkArgument(!StringUtils.isEmpty(uri.getHost()), "Path %s is missing bucket.", new Object[] { path });
        return path;
    }
    
    private static void checkPathMetadata(final PathMetadata meta) {
        Preconditions.checkNotNull((Object)meta);
        Preconditions.checkNotNull((Object)meta.getFileStatus());
        Preconditions.checkNotNull((Object)meta.getFileStatus().getPath());
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)DynamoDBMetadataStore.class);
        DynamoDBMetadataStore.deleteTrackingValueMap = new ValueMap().withBoolean(":false", false);
    }
}
