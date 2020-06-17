// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

public class RenameFailedException extends PathIOException
{
    private boolean exitCode;
    
    public RenameFailedException(final String src, final String dest, final Throwable cause) {
        super(src, cause);
        this.exitCode = false;
        this.setOperation("rename");
        this.setTargetPath(dest);
    }
    
    public RenameFailedException(final String src, final String dest, final String error) {
        super(src, error);
        this.exitCode = false;
        this.setOperation("rename");
        this.setTargetPath(dest);
    }
    
    public RenameFailedException(final Path src, final Path optionalDest, final String error) {
        super(src.toString(), error);
        this.exitCode = false;
        this.setOperation("rename");
        if (optionalDest != null) {
            this.setTargetPath(optionalDest.toString());
        }
    }
    
    public boolean getExitCode() {
        return this.exitCode;
    }
    
    public RenameFailedException withExitCode(final boolean code) {
        this.exitCode = code;
        return this;
    }
}
