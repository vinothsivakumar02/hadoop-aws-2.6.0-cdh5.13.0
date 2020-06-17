// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.util.HashMap;
import java.util.Map;

public enum Statistic
{
    DIRECTORIES_CREATED("directories_created", "Total number of directories created through the object store."), 
    DIRECTORIES_DELETED("directories_deleted", "Total number of directories deleted through the object store."), 
    FILES_COPIED("files_copied", "Total number of files copied within the object store."), 
    FILES_COPIED_BYTES("files_copied_bytes", "Total number of bytes copied within the object store."), 
    FILES_CREATED("files_created", "Total number of files created through the object store."), 
    FILES_DELETED("files_deleted", "Total number of files deleted from the object store."), 
    FAKE_DIRECTORIES_CREATED("fake_directories_created", "Total number of fake directory entries created in the object store."), 
    FAKE_DIRECTORIES_DELETED("fake_directories_deleted", "Total number of fake directory deletes submitted to object store."), 
    IGNORED_ERRORS("ignored_errors", "Errors caught and ignored"), 
    INVOCATION_COPY_FROM_LOCAL_FILE("op_copy_from_local_file", "Calls of copyFromLocalFile()"), 
    INVOCATION_EXISTS("op_exists", "Calls of exists()"), 
    INVOCATION_GET_FILE_STATUS("op_get_file_status", "Calls of getFileStatus()"), 
    INVOCATION_GLOB_STATUS("op_glob_status", "Calls of globStatus()"), 
    INVOCATION_IS_DIRECTORY("op_is_directory", "Calls of isDirectory()"), 
    INVOCATION_IS_FILE("op_is_file", "Calls of isFile()"), 
    INVOCATION_LIST_FILES("op_list_files", "Calls of listFiles()"), 
    INVOCATION_LIST_LOCATED_STATUS("op_list_located_status", "Calls of listLocatedStatus()"), 
    INVOCATION_LIST_STATUS("op_list_status", "Calls of listStatus()"), 
    INVOCATION_MKDIRS("op_mkdirs", "Calls of mkdirs()"), 
    INVOCATION_RENAME("op_rename", "Calls of rename()"), 
    OBJECT_COPY_REQUESTS("object_copy_requests", "Object copy requests"), 
    OBJECT_DELETE_REQUESTS("object_delete_requests", "Object delete requests"), 
    OBJECT_LIST_REQUESTS("object_list_requests", "Number of object listings made"), 
    OBJECT_CONTINUE_LIST_REQUESTS("object_continue_list_requests", "Number of continued object listings made"), 
    OBJECT_METADATA_REQUESTS("object_metadata_requests", "Number of requests for object metadata"), 
    OBJECT_MULTIPART_UPLOAD_ABORTED("object_multipart_aborted", "Object multipart upload aborted"), 
    OBJECT_PUT_REQUESTS("object_put_requests", "Object put/multipart upload count"), 
    OBJECT_PUT_REQUESTS_COMPLETED("object_put_requests_completed", "Object put/multipart upload completed count"), 
    OBJECT_PUT_REQUESTS_ACTIVE("object_put_requests_active", "Current number of active put requests"), 
    OBJECT_PUT_BYTES("object_put_bytes", "number of bytes uploaded"), 
    OBJECT_PUT_BYTES_PENDING("object_put_bytes_pending", "number of bytes queued for upload/being actively uploaded"), 
    STREAM_ABORTED("stream_aborted", "Count of times the TCP stream was aborted"), 
    STREAM_BACKWARD_SEEK_OPERATIONS("stream_backward_seek_operations", "Number of executed seek operations which went backwards in a stream"), 
    STREAM_CLOSED("stream_closed", "Count of times the TCP stream was closed"), 
    STREAM_CLOSE_OPERATIONS("stream_close_operations", "Total count of times an attempt to close a data stream was made"), 
    STREAM_FORWARD_SEEK_OPERATIONS("stream_forward_seek_operations", "Number of executed seek operations which went forward in a stream"), 
    STREAM_OPENED("stream_opened", "Total count of times an input stream to object store was opened"), 
    STREAM_READ_EXCEPTIONS("stream_read_exceptions", "Number of seek operations invoked on input streams"), 
    STREAM_READ_FULLY_OPERATIONS("stream_read_fully_operations", "Count of readFully() operations in streams"), 
    STREAM_READ_OPERATIONS("stream_read_operations", "Count of read() operations in streams"), 
    STREAM_READ_OPERATIONS_INCOMPLETE("stream_read_operations_incomplete", "Count of incomplete read() operations in streams"), 
    STREAM_SEEK_BYTES_BACKWARDS("stream_bytes_backwards_on_seek", "Count of bytes moved backwards during seek operations"), 
    STREAM_SEEK_BYTES_READ("stream_bytes_read", "Count of bytes read during seek() in stream operations"), 
    STREAM_SEEK_BYTES_SKIPPED("stream_bytes_skipped_on_seek", "Count of bytes skipped during forward seek operation"), 
    STREAM_SEEK_OPERATIONS("stream_seek_operations", "Number of seek operations during stream IO."), 
    STREAM_CLOSE_BYTES_READ("stream_bytes_read_in_close", "Count of bytes read when closing streams during seek operations."), 
    STREAM_ABORT_BYTES_DISCARDED("stream_bytes_discarded_in_abort", "Count of bytes discarded by aborting the stream"), 
    STREAM_WRITE_FAILURES("stream_write_failures", "Count of stream write failures reported"), 
    STREAM_WRITE_BLOCK_UPLOADS("stream_write_block_uploads", "Count of block/partition uploads completed"), 
    STREAM_WRITE_BLOCK_UPLOADS_ACTIVE("stream_write_block_uploads_active", "Count of block/partition uploads completed"), 
    STREAM_WRITE_BLOCK_UPLOADS_COMMITTED("stream_write_block_uploads_committed", "Count of number of block uploads committed"), 
    STREAM_WRITE_BLOCK_UPLOADS_ABORTED("stream_write_block_uploads_aborted", "Count of number of block uploads aborted"), 
    STREAM_WRITE_BLOCK_UPLOADS_PENDING("stream_write_block_uploads_pending", "Gauge of block/partitions uploads queued to be written"), 
    STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING("stream_write_block_uploads_data_pending", "Gauge of block/partitions data uploads queued to be written"), 
    STREAM_WRITE_TOTAL_TIME("stream_write_total_time", "Count of total time taken for uploads to complete"), 
    STREAM_WRITE_TOTAL_DATA("stream_write_total_data", "Count of total data uploaded in block output"), 
    STREAM_WRITE_QUEUE_DURATION("stream_write_queue_duration", "Total queue duration of all block uploads"), 
    S3GUARD_METADATASTORE_PUT_PATH_REQUEST("s3guard_metadatastore_put_path_request", "s3guard metadata store put one metadata path request"), 
    S3GUARD_METADATASTORE_PUT_PATH_LATENCY("s3guard_metadatastore_put_path_latency", "s3guard metadata store put one metadata path lantency"), 
    S3GUARD_METADATASTORE_INITIALIZATION("s3guard_metadatastore_initialization", "s3guard metadata store initialization times");
    
    private static final Map<String, Statistic> SYMBOL_MAP;
    private final String symbol;
    private final String description;
    
    private Statistic(final String symbol, final String description) {
        this.symbol = symbol;
        this.description = description;
    }
    
    public String getSymbol() {
        return this.symbol;
    }
    
    public static Statistic fromSymbol(final String symbol) {
        return Statistic.SYMBOL_MAP.get(symbol);
    }
    
    public String getDescription() {
        return this.description;
    }
    
    @Override
    public String toString() {
        return this.symbol;
    }
    
    static {
        SYMBOL_MAP = new HashMap<String, Statistic>(values().length);
        for (final Statistic stat : values()) {
            Statistic.SYMBOL_MAP.put(stat.getSymbol(), stat);
        }
    }
}
