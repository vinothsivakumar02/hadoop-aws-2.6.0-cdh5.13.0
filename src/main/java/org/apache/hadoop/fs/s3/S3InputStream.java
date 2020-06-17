// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import org.apache.commons.logging.LogFactory;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileSystem;
import java.io.DataInputStream;
import java.io.File;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSInputStream;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
class S3InputStream extends FSInputStream
{
    private FileSystemStore store;
    private Block[] blocks;
    private boolean closed;
    private long fileLength;
    private long pos;
    private File blockFile;
    private DataInputStream blockStream;
    private long blockEnd;
    private FileSystem.Statistics stats;
    private static final Log LOG;
    
    @Deprecated
    public S3InputStream(final Configuration conf, final FileSystemStore store, final INode inode) {
        this(conf, store, inode, null);
    }
    
    public S3InputStream(final Configuration conf, final FileSystemStore store, final INode inode, final FileSystem.Statistics stats) {
        this.pos = 0L;
        this.blockEnd = -1L;
        this.store = store;
        this.stats = stats;
        this.blocks = inode.getBlocks();
        for (final Block block : this.blocks) {
            this.fileLength += block.getLength();
        }
    }
    
    public synchronized long getPos() throws IOException {
        return this.pos;
    }
    
    public synchronized int available() throws IOException {
        return (int)(this.fileLength - this.pos);
    }
    
    public synchronized void seek(final long targetPos) throws IOException {
        final String message = String.format("Cannot seek to %d", targetPos);
        if (targetPos > this.fileLength) {
            throw new EOFException(message + ": after EOF");
        }
        if (targetPos < 0L) {
            throw new EOFException(message + ": negative");
        }
        this.pos = targetPos;
        this.blockEnd = -1L;
    }
    
    public synchronized boolean seekToNewSource(final long targetPos) throws IOException {
        return false;
    }
    
    public synchronized int read() throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        int result = -1;
        if (this.pos < this.fileLength) {
            if (this.pos > this.blockEnd) {
                this.blockSeekTo(this.pos);
            }
            result = this.blockStream.read();
            if (result >= 0) {
                ++this.pos;
            }
        }
        if (this.stats != null && result >= 0) {
            this.stats.incrementBytesRead(1L);
        }
        return result;
    }
    
    public synchronized int read(final byte[] buf, final int off, final int len) throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        if (this.pos < this.fileLength) {
            if (this.pos > this.blockEnd) {
                this.blockSeekTo(this.pos);
            }
            final int realLen = (int)Math.min(len, this.blockEnd - this.pos + 1L);
            final int result = this.blockStream.read(buf, off, realLen);
            if (result >= 0) {
                this.pos += result;
            }
            if (this.stats != null && result > 0) {
                this.stats.incrementBytesRead((long)result);
            }
            return result;
        }
        return -1;
    }
    
    private synchronized void blockSeekTo(final long target) throws IOException {
        int targetBlock = -1;
        long targetBlockStart = 0L;
        long targetBlockEnd = 0L;
        for (int i = 0; i < this.blocks.length; ++i) {
            final long blockLength = this.blocks[i].getLength();
            targetBlockEnd = targetBlockStart + blockLength - 1L;
            if (target >= targetBlockStart && target <= targetBlockEnd) {
                targetBlock = i;
                break;
            }
            targetBlockStart = targetBlockEnd + 1L;
        }
        if (targetBlock < 0) {
            throw new IOException("Impossible situation: could not find target position " + target);
        }
        final long offsetIntoBlock = target - targetBlockStart;
        this.blockFile = this.store.retrieveBlock(this.blocks[targetBlock], offsetIntoBlock);
        this.pos = target;
        this.blockEnd = targetBlockEnd;
        this.blockStream = new DataInputStream(new FileInputStream(this.blockFile));
    }
    
    public void close() throws IOException {
        if (this.closed) {
            return;
        }
        if (this.blockStream != null) {
            this.blockStream.close();
            this.blockStream = null;
        }
        if (this.blockFile != null) {
            final boolean b = this.blockFile.delete();
            if (!b) {
                S3InputStream.LOG.warn((Object)"Ignoring failed delete");
            }
        }
        super.close();
        this.closed = true;
    }
    
    public boolean markSupported() {
        return false;
    }
    
    public void mark(final int readLimit) {
    }
    
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
    
    static {
        LOG = LogFactory.getLog(S3InputStream.class.getName());
    }
}
