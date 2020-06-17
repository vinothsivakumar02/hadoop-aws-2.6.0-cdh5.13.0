// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import org.apache.hadoop.io.IOUtils;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
public class INode
{
    public static final FileType[] FILE_TYPES;
    public static final INode DIRECTORY_INODE;
    private FileType fileType;
    private Block[] blocks;
    
    public INode(final FileType fileType, final Block[] blocks) {
        this.fileType = fileType;
        if (this.isDirectory() && blocks != null) {
            throw new IllegalArgumentException("A directory cannot contain blocks.");
        }
        this.blocks = blocks;
    }
    
    public Block[] getBlocks() {
        return this.blocks;
    }
    
    public FileType getFileType() {
        return this.fileType;
    }
    
    public boolean isDirectory() {
        return this.fileType == FileType.DIRECTORY;
    }
    
    public boolean isFile() {
        return this.fileType == FileType.FILE;
    }
    
    public long getSerializedLength() {
        return 1L + ((this.blocks == null) ? 0 : (4 + this.blocks.length * 16));
    }
    
    public InputStream serialize() throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        try {
            out.writeByte(this.fileType.ordinal());
            if (this.isFile()) {
                out.writeInt(this.blocks.length);
                for (int i = 0; i < this.blocks.length; ++i) {
                    out.writeLong(this.blocks[i].getId());
                    out.writeLong(this.blocks[i].getLength());
                }
            }
            out.close();
            out = null;
        }
        finally {
            IOUtils.closeStream((Closeable)out);
        }
        return new ByteArrayInputStream(bytes.toByteArray());
    }
    
    public static INode deserialize(final InputStream in) throws IOException {
        if (in == null) {
            return null;
        }
        final DataInputStream dataIn = new DataInputStream(in);
        final FileType fileType = INode.FILE_TYPES[dataIn.readByte()];
        switch (fileType) {
            case DIRECTORY: {
                in.close();
                return INode.DIRECTORY_INODE;
            }
            case FILE: {
                final int numBlocks = dataIn.readInt();
                final Block[] blocks = new Block[numBlocks];
                for (int i = 0; i < numBlocks; ++i) {
                    final long id = dataIn.readLong();
                    final long length = dataIn.readLong();
                    blocks[i] = new Block(id, length);
                }
                in.close();
                return new INode(fileType, blocks);
            }
            default: {
                throw new IllegalArgumentException("Cannot deserialize inode.");
            }
        }
    }
    
    static {
        FILE_TYPES = new FileType[] { FileType.DIRECTORY, FileType.FILE };
        DIRECTORY_INODE = new INode(FileType.DIRECTORY, null);
    }
    
    enum FileType
    {
        DIRECTORY, 
        FILE;
    }
}
