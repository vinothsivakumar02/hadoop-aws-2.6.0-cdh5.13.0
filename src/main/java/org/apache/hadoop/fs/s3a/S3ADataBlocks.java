// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.util.DirectBufferPool;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.File;
import java.io.Closeable;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;

final class S3ADataBlocks
{
    private static final Logger LOG;
    
    private S3ADataBlocks() {
    }
    
    static void validateWriteArgs(final byte[] b, final int off, final int len) throws IOException {
        Preconditions.checkNotNull((Object)b);
        if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) {
            throw new IndexOutOfBoundsException("write (b[" + b.length + "], " + off + ", " + len + ')');
        }
    }
    
    static BlockFactory createFactory(final S3AFileSystem owner, final String name) {
        switch (name) {
            case "array": {
                return new ArrayBlockFactory(owner);
            }
            case "disk": {
                return new DiskBlockFactory(owner);
            }
            case "bytebuffer": {
                return new ByteBufferBlockFactory(owner);
            }
            default: {
                throw new IllegalArgumentException("Unsupported block buffer \"" + name + '\"');
            }
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3ADataBlocks.class);
    }
    
    static final class BlockUploadData implements Closeable
    {
        private final File file;
        private final InputStream uploadStream;
        
        BlockUploadData(final File file) {
            Preconditions.checkArgument(file.exists(), (Object)("No file: " + file));
            this.file = file;
            this.uploadStream = null;
        }
        
        BlockUploadData(final InputStream uploadStream) {
            Preconditions.checkNotNull((Object)uploadStream, (Object)"rawUploadStream");
            this.uploadStream = uploadStream;
            this.file = null;
        }
        
        boolean hasFile() {
            return this.file != null;
        }
        
        File getFile() {
            return this.file;
        }
        
        InputStream getUploadStream() {
            return this.uploadStream;
        }
        
        @Override
        public void close() throws IOException {
            S3AUtils.closeAll(S3ADataBlocks.LOG, this.uploadStream);
        }
    }
    
    abstract static class BlockFactory implements Closeable
    {
        private final S3AFileSystem owner;
        
        protected BlockFactory(final S3AFileSystem owner) {
            this.owner = owner;
        }
        
        abstract DataBlock create(final long p0, final int p1, final S3AInstrumentation.OutputStreamStatistics p2) throws IOException;
        
        @Override
        public void close() throws IOException {
        }
        
        protected S3AFileSystem getOwner() {
            return this.owner;
        }
    }
    
    abstract static class DataBlock implements Closeable
    {
        private volatile DestState state;
        protected final long index;
        protected final S3AInstrumentation.OutputStreamStatistics statistics;
        
        protected DataBlock(final long index, final S3AInstrumentation.OutputStreamStatistics statistics) {
            this.state = DestState.Writing;
            this.index = index;
            this.statistics = statistics;
        }
        
        protected final synchronized void enterState(final DestState current, final DestState next) throws IllegalStateException {
            this.verifyState(current);
            S3ADataBlocks.LOG.debug("{}: entering state {}", (Object)this, (Object)next);
            this.state = next;
        }
        
        protected final void verifyState(final DestState expected) throws IllegalStateException {
            if (expected != null && this.state != expected) {
                throw new IllegalStateException("Expected stream state " + expected + " -but actual state is " + this.state + " in " + this);
            }
        }
        
        final DestState getState() {
            return this.state;
        }
        
        abstract int dataSize();
        
        abstract boolean hasCapacity(final long p0);
        
        boolean hasData() {
            return this.dataSize() > 0;
        }
        
        abstract int remainingCapacity();
        
        int write(final byte[] buffer, final int offset, final int length) throws IOException {
            this.verifyState(DestState.Writing);
            Preconditions.checkArgument(buffer != null, (Object)"Null buffer");
            Preconditions.checkArgument(length >= 0, (Object)"length is negative");
            Preconditions.checkArgument(offset >= 0, (Object)"offset is negative");
            Preconditions.checkArgument(buffer.length - offset >= length, (Object)"buffer shorter than amount of data to write");
            return 0;
        }
        
        void flush() throws IOException {
            this.verifyState(DestState.Writing);
        }
        
        BlockUploadData startUpload() throws IOException {
            S3ADataBlocks.LOG.debug("Start datablock[{}] upload", (Object)this.index);
            this.enterState(DestState.Writing, DestState.Upload);
            return null;
        }
        
        protected synchronized boolean enterClosedState() {
            if (!this.state.equals(DestState.Closed)) {
                this.enterState(null, DestState.Closed);
                return true;
            }
            return false;
        }
        
        @Override
        public void close() throws IOException {
            if (this.enterClosedState()) {
                S3ADataBlocks.LOG.debug("Closed {}", (Object)this);
                this.innerClose();
            }
        }
        
        protected void innerClose() throws IOException {
        }
        
        protected void blockAllocated() {
            if (this.statistics != null) {
                this.statistics.blockAllocated();
            }
        }
        
        protected void blockReleased() {
            if (this.statistics != null) {
                this.statistics.blockReleased();
            }
        }
        
        enum DestState
        {
            Writing, 
            Upload, 
            Closed;
        }
    }
    
    static class ArrayBlockFactory extends BlockFactory
    {
        ArrayBlockFactory(final S3AFileSystem owner) {
            super(owner);
        }
        
        @Override
        DataBlock create(final long index, final int limit, final S3AInstrumentation.OutputStreamStatistics statistics) throws IOException {
            return new ByteArrayBlock(0L, limit, statistics);
        }
    }
    
    static class S3AByteArrayOutputStream extends ByteArrayOutputStream
    {
        S3AByteArrayOutputStream(final int size) {
            super(size);
        }
        
        ByteArrayInputStream getInputStream() {
            final ByteArrayInputStream bin = new ByteArrayInputStream(this.buf, 0, this.count);
            this.reset();
            this.buf = null;
            return bin;
        }
    }
    
    static class ByteArrayBlock extends DataBlock
    {
        private S3AByteArrayOutputStream buffer;
        private final int limit;
        private Integer dataSize;
        
        ByteArrayBlock(final long index, final int limit, final S3AInstrumentation.OutputStreamStatistics statistics) {
            super(index, statistics);
            this.limit = limit;
            this.buffer = new S3AByteArrayOutputStream(limit);
            this.blockAllocated();
        }
        
        @Override
        int dataSize() {
            return (this.dataSize != null) ? this.dataSize : this.buffer.size();
        }
        
        @Override
        BlockUploadData startUpload() throws IOException {
            super.startUpload();
            this.dataSize = this.buffer.size();
            final ByteArrayInputStream bufferData = this.buffer.getInputStream();
            this.buffer = null;
            return new BlockUploadData(bufferData);
        }
        
        @Override
        boolean hasCapacity(final long bytes) {
            return this.dataSize() + bytes <= this.limit;
        }
        
        @Override
        int remainingCapacity() {
            return this.limit - this.dataSize();
        }
        
        @Override
        int write(final byte[] b, final int offset, final int len) throws IOException {
            super.write(b, offset, len);
            final int written = Math.min(this.remainingCapacity(), len);
            this.buffer.write(b, offset, written);
            return written;
        }
        
        @Override
        protected void innerClose() {
            this.buffer = null;
            this.blockReleased();
        }
        
        @Override
        public String toString() {
            return "ByteArrayBlock{index=" + this.index + ", state=" + this.getState() + ", limit=" + this.limit + ", dataSize=" + this.dataSize + '}';
        }
    }
    
    static class ByteBufferBlockFactory extends BlockFactory
    {
        private final DirectBufferPool bufferPool;
        private final AtomicInteger buffersOutstanding;
        
        ByteBufferBlockFactory(final S3AFileSystem owner) {
            super(owner);
            this.bufferPool = new DirectBufferPool();
            this.buffersOutstanding = new AtomicInteger(0);
        }
        
        @Override
        ByteBufferBlock create(final long index, final int limit, final S3AInstrumentation.OutputStreamStatistics statistics) throws IOException {
            return new ByteBufferBlock(index, limit, statistics);
        }
        
        private ByteBuffer requestBuffer(final int limit) {
            S3ADataBlocks.LOG.debug("Requesting buffer of size {}", (Object)limit);
            this.buffersOutstanding.incrementAndGet();
            return this.bufferPool.getBuffer(limit);
        }
        
        private void releaseBuffer(final ByteBuffer buffer) {
            S3ADataBlocks.LOG.debug("Releasing buffer");
            this.bufferPool.returnBuffer(buffer);
            this.buffersOutstanding.decrementAndGet();
        }
        
        public int getOutstandingBufferCount() {
            return this.buffersOutstanding.get();
        }
        
        @Override
        public String toString() {
            return "ByteBufferBlockFactory{buffersOutstanding=" + this.buffersOutstanding + '}';
        }
        
        class ByteBufferBlock extends DataBlock
        {
            private ByteBuffer blockBuffer;
            private final int bufferSize;
            private Integer dataSize;
            
            ByteBufferBlock(final long index, final int bufferSize, final S3AInstrumentation.OutputStreamStatistics statistics) {
                super(index, statistics);
                this.bufferSize = bufferSize;
                this.blockBuffer = ByteBufferBlockFactory.this.requestBuffer(bufferSize);
                this.blockAllocated();
            }
            
            @Override
            int dataSize() {
                return (this.dataSize != null) ? this.dataSize : this.bufferCapacityUsed();
            }
            
            @Override
            BlockUploadData startUpload() throws IOException {
                super.startUpload();
                this.dataSize = this.bufferCapacityUsed();
                this.blockBuffer.limit(this.blockBuffer.position());
                this.blockBuffer.position(0);
                return new BlockUploadData(new ByteBufferInputStream(this.dataSize, this.blockBuffer));
            }
            
            public boolean hasCapacity(final long bytes) {
                return bytes <= this.remainingCapacity();
            }
            
            public int remainingCapacity() {
                return (this.blockBuffer != null) ? this.blockBuffer.remaining() : 0;
            }
            
            private int bufferCapacityUsed() {
                return this.blockBuffer.capacity() - this.blockBuffer.remaining();
            }
            
            @Override
            int write(final byte[] b, final int offset, final int len) throws IOException {
                super.write(b, offset, len);
                final int written = Math.min(this.remainingCapacity(), len);
                this.blockBuffer.put(b, offset, written);
                return written;
            }
            
            @Override
            protected void innerClose() {
                if (this.blockBuffer != null) {
                    this.blockReleased();
                    ByteBufferBlockFactory.this.releaseBuffer(this.blockBuffer);
                    this.blockBuffer = null;
                }
            }
            
            @Override
            public String toString() {
                return "ByteBufferBlock{index=" + this.index + ", state=" + this.getState() + ", dataSize=" + this.dataSize() + ", limit=" + this.bufferSize + ", remainingCapacity=" + this.remainingCapacity() + '}';
            }
            
            class ByteBufferInputStream extends InputStream
            {
                private final int size;
                private ByteBuffer byteBuffer;
                
                ByteBufferInputStream(final int size, final ByteBuffer byteBuffer) {
                    S3ADataBlocks.LOG.debug("Creating ByteBufferInputStream of size {}", (Object)size);
                    this.size = size;
                    this.byteBuffer = byteBuffer;
                }
                
                @Override
                public synchronized void close() {
                    //S3ADataBlocks.LOG.debug("ByteBufferInputStream.close() for {}", (Object)Object.this.toString());
                    this.byteBuffer = null;
                }
                
                private void verifyOpen() throws IOException {
                    if (this.byteBuffer == null) {
                        throw new IOException("Stream is closed!");
                    }
                }
                
                @Override
                public synchronized int read() throws IOException {
                    if (this.available() > 0) {
                        return this.byteBuffer.get() & 0xFF;
                    }
                    return -1;
                }
                
                @Override
                public synchronized long skip(final long offset) throws IOException {
                    this.verifyOpen();
                    final long newPos = this.position() + offset;
                    if (newPos < 0L) {
                        throw new EOFException("Cannot seek to a negative offset");
                    }
                    if (newPos > this.size) {
                        throw new EOFException("Attempted to seek or read past the end of the file");
                    }
                    this.byteBuffer.position((int)newPos);
                    return newPos;
                }
                
                @Override
                public synchronized int available() {
                    Preconditions.checkState(this.byteBuffer != null, (Object)"Stream is closed!");
                    return this.byteBuffer.remaining();
                }
                
                public synchronized int position() {
                    return this.byteBuffer.position();
                }
                
                public synchronized boolean hasRemaining() {
                    return this.byteBuffer.hasRemaining();
                }
                
                @Override
                public synchronized void mark(final int readlimit) {
                    S3ADataBlocks.LOG.debug("mark at {}", (Object)this.position());
                    this.byteBuffer.mark();
                }
                
                @Override
                public synchronized void reset() throws IOException {
                    S3ADataBlocks.LOG.debug("reset");
                    this.byteBuffer.reset();
                }
                
                @Override
                public boolean markSupported() {
                    return true;
                }
                
                @Override
                public synchronized int read(final byte[] b, final int offset, final int length) throws IOException {
                    Preconditions.checkArgument(length >= 0, (Object)"length is negative");
                    Preconditions.checkArgument(b != null, (Object)"Null buffer");
                    if (b.length - offset < length) {
                        throw new IndexOutOfBoundsException("Requested more bytes than destination buffer size: request length =" + length + ", with offset =" + offset + "; buffer capacity =" + (b.length - offset));
                    }
                    this.verifyOpen();
                    if (!this.hasRemaining()) {
                        return -1;
                    }
                    final int toRead = Math.min(length, this.available());
                    this.byteBuffer.get(b, offset, toRead);
                    return toRead;
                }
                
                @Override
                public String toString() {
                    final StringBuilder sb = new StringBuilder("ByteBufferInputStream{");
                    sb.append("size=").append(this.size);
                    final ByteBuffer buf = this.byteBuffer;
                    if (buf != null) {
                        sb.append(", available=").append(buf.remaining());
                    }
                    sb.append(", ").append(this.toString());
                    sb.append('}');
                    return sb.toString();
                }
            }
        }
    }
    
    static class DiskBlockFactory extends BlockFactory
    {
        DiskBlockFactory(final S3AFileSystem owner) {
            super(owner);
        }
        
        @Override
        DataBlock create(final long index, final int limit, final S3AInstrumentation.OutputStreamStatistics statistics) throws IOException {
            final File destFile = this.getOwner().createTmpFileForWrite(String.format("s3ablock-%04d-", index), limit, this.getOwner().getConf());
            return new DiskBlock(destFile, limit, index, statistics);
        }
    }
    
    static class DiskBlock extends DataBlock
    {
        private int bytesWritten;
        private final File bufferFile;
        private final int limit;
        private BufferedOutputStream out;
        private final AtomicBoolean closed;
        
        DiskBlock(final File bufferFile, final int limit, final long index, final S3AInstrumentation.OutputStreamStatistics statistics) throws FileNotFoundException {
            super(index, statistics);
            this.closed = new AtomicBoolean(false);
            this.limit = limit;
            this.bufferFile = bufferFile;
            this.blockAllocated();
            this.out = new BufferedOutputStream(new FileOutputStream(bufferFile));
        }
        
        @Override
        int dataSize() {
            return this.bytesWritten;
        }
        
        @Override
        boolean hasCapacity(final long bytes) {
            return this.dataSize() + bytes <= this.limit;
        }
        
        @Override
        int remainingCapacity() {
            return this.limit - this.bytesWritten;
        }
        
        @Override
        int write(final byte[] b, final int offset, final int len) throws IOException {
            super.write(b, offset, len);
            final int written = Math.min(this.remainingCapacity(), len);
            this.out.write(b, offset, written);
            this.bytesWritten += written;
            return written;
        }
        
        @Override
        BlockUploadData startUpload() throws IOException {
            super.startUpload();
            try {
                this.out.flush();
            }
            finally {
                this.out.close();
                this.out = null;
            }
            return new BlockUploadData(this.bufferFile);
        }
        
        @Override
        protected void innerClose() throws IOException {
            final DestState state = this.getState();
            S3ADataBlocks.LOG.debug("Closing {}", (Object)this);
            switch (state) {
                case Writing: {
                    if (this.bufferFile.exists()) {
                        S3ADataBlocks.LOG.debug("Block[{}]: Deleting buffer file as upload did not start", (Object)this.index);
                        this.closeBlock();
                        break;
                    }
                    break;
                }
                case Upload: {
                    S3ADataBlocks.LOG.debug("Block[{}]: Buffer file {} exists \u2014close upload stream", (Object)this.index, (Object)this.bufferFile);
                    break;
                }
                case Closed: {
                    this.closeBlock();
                    break;
                }
            }
        }
        
        @Override
        void flush() throws IOException {
            super.flush();
            this.out.flush();
        }
        
        @Override
        public String toString() {
            final String sb = "FileBlock{index=" + this.index + ", destFile=" + this.bufferFile + ", state=" + this.getState() + ", dataSize=" + this.dataSize() + ", limit=" + this.limit + '}';
            return sb;
        }
        
        void closeBlock() {
            S3ADataBlocks.LOG.debug("block[{}]: closeBlock()", (Object)this.index);
            if (!this.closed.getAndSet(true)) {
                this.blockReleased();
                if (!this.bufferFile.delete() && this.bufferFile.exists()) {
                    S3ADataBlocks.LOG.warn("delete({}) returned false", (Object)this.bufferFile.getAbsoluteFile());
                }
            }
            else {
                S3ADataBlocks.LOG.debug("block[{}]: skipping re-entrant closeBlock()", (Object)this.index);
            }
        }
    }
}
