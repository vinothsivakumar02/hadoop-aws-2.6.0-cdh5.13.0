// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import org.apache.hadoop.util.Progressable;
import org.apache.commons.logging.Log;
import java.util.List;
import java.util.Random;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.OutputStream;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
class S3OutputStream extends OutputStream
{
    private Configuration conf;
    private int bufferSize;
    private FileSystemStore store;
    private Path path;
    private long blockSize;
    private File backupFile;
    private OutputStream backupStream;
    private Random r;
    private boolean closed;
    private int pos;
    private long filePos;
    private int bytesWrittenToBlock;
    private byte[] outBuf;
    private List<Block> blocks;
    private Block nextBlock;
    private static final Log LOG;
    
    public S3OutputStream(final Configuration conf, final FileSystemStore store, final Path path, final long blockSize, final Progressable progress, final int buffersize) throws IOException {
        this.r = new Random();
        this.pos = 0;
        this.filePos = 0L;
        this.bytesWrittenToBlock = 0;
        this.blocks = new ArrayList<Block>();
        this.conf = conf;
        this.store = store;
        this.path = path;
        this.blockSize = blockSize;
        this.backupFile = this.newBackupFile();
        this.backupStream = new FileOutputStream(this.backupFile);
        this.bufferSize = buffersize;
        this.outBuf = new byte[this.bufferSize];
    }
    
    private File newBackupFile() throws IOException {
        final File dir = new File(this.conf.get("fs.s3.buffer.dir"));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create S3 buffer directory: " + dir);
        }
        final File result = File.createTempFile("output-", ".tmp", dir);
        result.deleteOnExit();
        return result;
    }
    
    public long getPos() throws IOException {
        return this.filePos;
    }
    
    @Override
    public synchronized void write(final int b) throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        if (this.bytesWrittenToBlock + this.pos == this.blockSize || this.pos >= this.bufferSize) {
            this.flush();
        }
        this.outBuf[this.pos++] = (byte)b;
        ++this.filePos;
    }
    
    @Override
    public synchronized void write(final byte[] b, int off, int len) throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        while (len > 0) {
            final int remaining = this.bufferSize - this.pos;
            final int toWrite = Math.min(remaining, len);
            System.arraycopy(b, off, this.outBuf, this.pos, toWrite);
            this.pos += toWrite;
            off += toWrite;
            len -= toWrite;
            this.filePos += toWrite;
            if (this.bytesWrittenToBlock + this.pos >= this.blockSize || this.pos == this.bufferSize) {
                this.flush();
            }
        }
    }
    
    @Override
    public synchronized void flush() throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        if (this.bytesWrittenToBlock + this.pos >= this.blockSize) {
            this.flushData((int)this.blockSize - this.bytesWrittenToBlock);
        }
        if (this.bytesWrittenToBlock == this.blockSize) {
            this.endBlock();
        }
        this.flushData(this.pos);
    }
    
    private synchronized void flushData(final int maxPos) throws IOException {
        final int workingPos = Math.min(this.pos, maxPos);
        if (workingPos > 0) {
            this.backupStream.write(this.outBuf, 0, workingPos);
            this.bytesWrittenToBlock += workingPos;
            System.arraycopy(this.outBuf, workingPos, this.outBuf, 0, this.pos - workingPos);
            this.pos -= workingPos;
        }
    }
    
    private synchronized void endBlock() throws IOException {
        this.backupStream.close();
        this.nextBlockOutputStream();
        this.store.storeBlock(this.nextBlock, this.backupFile);
        this.internalClose();
        final boolean b = this.backupFile.delete();
        if (!b) {
            S3OutputStream.LOG.warn((Object)"Ignoring failed delete");
        }
        this.backupFile = this.newBackupFile();
        this.backupStream = new FileOutputStream(this.backupFile);
        this.bytesWrittenToBlock = 0;
    }
    
    private synchronized void nextBlockOutputStream() throws IOException {
        long blockId;
        for (blockId = this.r.nextLong(); this.store.blockExists(blockId); blockId = this.r.nextLong()) {}
        this.nextBlock = new Block(blockId, this.bytesWrittenToBlock);
        this.blocks.add(this.nextBlock);
        this.bytesWrittenToBlock = 0;
    }
    
    private synchronized void internalClose() throws IOException {
        final INode inode = new INode(INode.FileType.FILE, this.blocks.toArray(new Block[this.blocks.size()]));
        this.store.storeINode(this.path, inode);
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (this.closed) {
            return;
        }
        this.flush();
        if (this.filePos == 0L || this.bytesWrittenToBlock != 0) {
            this.endBlock();
        }
        this.backupStream.close();
        final boolean b = this.backupFile.delete();
        if (!b) {
            S3OutputStream.LOG.warn((Object)"Ignoring failed delete");
        }
        super.close();
        this.closed = true;
    }
    
    static {
        LOG = LogFactory.getLog(S3OutputStream.class.getName());
    }
}
