// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import java.net.URI;
import org.apache.commons.lang.StringUtils;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import java.util.Iterator;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import java.io.IOException;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import com.google.common.base.Preconditions;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import java.util.Arrays;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import java.util.Collection;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class PathMetadataDynamoDBTranslation
{
    @VisibleForTesting
    static final String PARENT = "parent";
    @VisibleForTesting
    static final String CHILD = "child";
    @VisibleForTesting
    static final String IS_DIR = "is_dir";
    @VisibleForTesting
    static final String MOD_TIME = "mod_time";
    @VisibleForTesting
    static final String FILE_LENGTH = "file_length";
    @VisibleForTesting
    static final String BLOCK_SIZE = "block_size";
    static final String IS_DELETED = "is_deleted";
    @VisibleForTesting
    static final String TABLE_VERSION = "table_version";
    @VisibleForTesting
    static final String TABLE_CREATED = "table_created";
    static final String E_NOT_VERSION_MARKER = "Not a version marker: ";
    
    static Collection<KeySchemaElement> keySchema() {
        return Arrays.asList(new KeySchemaElement("parent", KeyType.HASH), new KeySchemaElement("child", KeyType.RANGE));
    }
    
    static Collection<AttributeDefinition> attributeDefinitions() {
        return Arrays.asList(new AttributeDefinition("parent", ScalarAttributeType.S), new AttributeDefinition("child", ScalarAttributeType.S));
    }
    
    static PathMetadata itemToPathMetadata(final Item item, final String username) throws IOException {
        if (item == null) {
            return null;
        }
        final String parentStr = item.getString("parent");
        Preconditions.checkNotNull((Object)parentStr, "No parent entry in item %s", new Object[] { item });
        final String childStr = item.getString("child");
        Preconditions.checkNotNull((Object)childStr, "No child entry in item %s", new Object[] { item });
        final Path rawPath = new Path(parentStr, childStr);
        if (!rawPath.isAbsoluteAndSchemeAuthorityNull()) {
            return null;
        }
        final Path parent = new Path("s3a:/" + parentStr + "/");
        final Path path = new Path(parent, childStr);
        final boolean isDir = item.hasAttribute("is_dir") && item.getBoolean("is_dir");
        FileStatus fileStatus;
        if (isDir) {
            fileStatus = DynamoDBMetadataStore.makeDirStatus(path, username);
        }
        else {
            final long len = item.hasAttribute("file_length") ? item.getLong("file_length") : 0L;
            final long modTime = item.hasAttribute("mod_time") ? item.getLong("mod_time") : 0L;
            final long block = item.hasAttribute("block_size") ? item.getLong("block_size") : 0L;
            fileStatus = new FileStatus(len, false, 1, block, modTime, 0L, (FsPermission)null, username, username, path);
        }
        final boolean isDeleted = item.hasAttribute("is_deleted") && item.getBoolean("is_deleted");
        return new PathMetadata(fileStatus, Tristate.UNKNOWN, isDeleted);
    }
    
    static Item pathMetadataToItem(final PathMetadata meta) {
        Preconditions.checkNotNull((Object)meta);
        final FileStatus status = meta.getFileStatus();
        final Item item = new Item().withPrimaryKey(pathToKey(status.getPath()));
        if (status.isDirectory()) {
            item.withBoolean("is_dir", true);
        }
        else {
            item.withLong("file_length", status.getLen()).withLong("mod_time", status.getModificationTime()).withLong("block_size", status.getBlockSize());
        }
        item.withBoolean("is_deleted", meta.isDeleted());
        return item;
    }
    
    static Item createVersionMarker(final String name, final int version, final long timestamp) {
        return new Item().withPrimaryKey(createVersionMarkerPrimaryKey(name)).withInt("table_version", version).withLong("table_created", timestamp);
    }
    
    static PrimaryKey createVersionMarkerPrimaryKey(final String name) {
        return new PrimaryKey("parent", (Object)name, "child", (Object)name);
    }
    
    static int extractVersionFromMarker(final Item marker) throws IOException {
        if (marker.hasAttribute("table_version")) {
            return marker.getInt("table_version");
        }
        throw new IOException("Not a version marker: " + marker);
    }
    
    static Long extractCreationTimeFromMarker(final Item marker) throws IOException {
        if (marker.hasAttribute("table_created")) {
            return marker.getLong("table_created");
        }
        return null;
    }
    
    static Item[] pathMetadataToItem(final Collection<PathMetadata> metas) {
        if (metas == null) {
            return null;
        }
        final Item[] items = new Item[metas.size()];
        int i = 0;
        for (final PathMetadata meta : metas) {
            items[i++] = pathMetadataToItem(meta);
        }
        return items;
    }
    
    static KeyAttribute pathToParentKeyAttribute(final Path path) {
        return new KeyAttribute("parent", (Object)pathToParentKey(path));
    }
    
    static String pathToParentKey(final Path path) {
        Preconditions.checkNotNull((Object)path);
        Preconditions.checkArgument(path.isUriPathAbsolute(), (Object)"Path not absolute");
        final URI uri = path.toUri();
        final String bucket = uri.getHost();
        Preconditions.checkArgument(!StringUtils.isEmpty(bucket), (Object)"Path missing bucket");
        String pKey = "/" + bucket + uri.getPath();
        if (pKey.endsWith("/")) {
            pKey = pKey.substring(0, pKey.length() - 1);
        }
        return pKey;
    }
    
    static PrimaryKey pathToKey(final Path path) {
        Preconditions.checkArgument(!path.isRoot(), (Object)"Root path is not mapped to any PrimaryKey");
        return new PrimaryKey("parent", (Object)pathToParentKey(path.getParent()), "child", (Object)path.getName());
    }
    
    static PrimaryKey[] pathToKey(final Collection<Path> paths) {
        if (paths == null) {
            return null;
        }
        final PrimaryKey[] keys = new PrimaryKey[paths.size()];
        int i = 0;
        for (final Path p : paths) {
            keys[i++] = pathToKey(p);
        }
        return keys;
    }
    
    private PathMetadataDynamoDBTranslation() {
    }
}
