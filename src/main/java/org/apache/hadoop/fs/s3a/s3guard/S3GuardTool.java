// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.concurrent.TimeUnit;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
import java.util.HashMap;
import java.io.PrintStream;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileStatus;
import java.util.HashSet;
import org.apache.hadoop.fs.Path;
import java.util.Set;
import com.google.common.base.Preconditions;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;
import java.io.IOException;
import java.util.List;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

public abstract class S3GuardTool extends Configured implements Tool
{
    private static final Logger LOG;
    private static final String NAME = "s3guard";
    private static final String COMMON_USAGE = "When possible and not overridden by more specific options, metadata\nrepository information will be inferred from the S3A URL (if provided)\n\nGeneric options supported are:\n  -conf <config file> - specify an application configuration file\n  -D <property=value> - define a value for a given property\n";
    private static final String USAGE = "s3guard [command] [OPTIONS] [s3a://BUCKET]\n\nCommands: \n\tinit - initialize metadata repository\n\tdestroy - destroy metadata repository\n\timport - import metadata from existing S3 data\n\tdiff - report on delta between S3 and repository\n\tprune - truncate older metadata from repository\n";
    static final int SUCCESS = 0;
    static final int INVALID_ARGUMENT = 1;
    static final int ERROR = 99;
    protected S3AFileSystem s3a;
    protected MetadataStore ms;
    protected CommandFormat commandFormat;
    private static final String META_FLAG = "meta";
    private static final String DAYS_FLAG = "days";
    private static final String HOURS_FLAG = "hours";
    private static final String MINUTES_FLAG = "minutes";
    private static final String SECONDS_FLAG = "seconds";
    private static final String REGION_FLAG = "region";
    private static final String READ_FLAG = "read";
    private static final String WRITE_FLAG = "write";
    private static S3GuardTool cmd;
    
    public abstract String getUsage();
    
    public S3GuardTool(final Configuration conf) {
        super(conf);
        (this.commandFormat = new CommandFormat(0, Integer.MAX_VALUE, new String[0])).addOptionWithValue("meta");
        this.commandFormat.addOptionWithValue("region");
    }
    
    abstract String getName();
    
    @VisibleForTesting
    public MetadataStore getMetadataStore() {
        return this.ms;
    }
    
    boolean parseDynamoDBRegion(final List<String> paths) throws IOException {
        final Configuration conf = this.getConf();
        final String fromCli = this.commandFormat.getOptValue("region");
        final String fromConf = conf.get("fs.s3a.s3guard.ddb.region");
        final boolean hasS3Path = !paths.isEmpty();
        if (fromCli != null) {
            if (fromCli.isEmpty()) {
                System.err.println("No region provided with -region flag");
                return false;
            }
            if (hasS3Path) {
                System.err.println("Providing both an S3 path and the -region flag is not supported. If you need to specify a different region than the S3 bucket, configure fs.s3a.s3guard.ddb.region");
                return false;
            }
            conf.set("fs.s3a.s3guard.ddb.region", fromCli);
            return true;
        }
        else if (fromConf != null) {
            if (fromConf.isEmpty()) {
                System.err.printf("No region provided with config %s, %n", "fs.s3a.s3guard.ddb.region");
                return false;
            }
            return true;
        }
        else {
            if (hasS3Path) {
                final String s3Path = paths.get(0);
                this.initS3AFileSystem(s3Path);
                return true;
            }
            System.err.println("No region found from -region flag, config, or S3 bucket");
            return false;
        }
    }
    
    MetadataStore initMetadataStore(final boolean forceCreate) throws IOException {
        if (this.ms != null) {
            return this.ms;
        }
        Configuration conf;
        if (this.s3a == null) {
            conf = this.getConf();
        }
        else {
            conf = this.s3a.getConf();
        }
        final String metaURI = this.commandFormat.getOptValue("meta");
        if (metaURI != null && !metaURI.isEmpty()) {
            final URI uri = URI.create(metaURI);
            S3GuardTool.LOG.info("create metadata store: {}", (Object)(uri + " scheme: " + uri.getScheme()));
            final String lowerCase = uri.getScheme().toLowerCase();
            switch (lowerCase) {
                case "local": {
                    this.ms = new LocalMetadataStore();
                    break;
                }
                case "dynamodb": {
                    this.ms = new DynamoDBMetadataStore();
                    conf.set("fs.s3a.s3guard.ddb.table", uri.getAuthority());
                    if (forceCreate) {
                        conf.setBoolean("fs.s3a.s3guard.ddb.table.create", true);
                        break;
                    }
                    break;
                }
                default: {
                    throw new IOException(String.format("Metadata store %s is not supported", uri));
                }
            }
        }
        else {
            this.ms = new DynamoDBMetadataStore();
            if (forceCreate) {
                conf.setBoolean("fs.s3a.s3guard.ddb.table.create", true);
            }
        }
        if (this.s3a == null) {
            this.ms.initialize(conf);
        }
        else {
            this.ms.initialize(this.s3a);
        }
        S3GuardTool.LOG.info("Metadata store {} is initialized.", (Object)this.ms);
        return this.ms;
    }
    
    void initS3AFileSystem(final String path) throws IOException {
        URI uri;
        try {
            uri = new URI(path);
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
        }
        final Configuration conf = this.getConf();
        conf.setClass("fs.s3a.metadatastore.impl", (Class)NullMetadataStore.class, (Class)MetadataStore.class);
        final FileSystem fs = FileSystem.get(uri, this.getConf());
        if (!(fs instanceof S3AFileSystem)) {
            throw new IOException(String.format("URI %s is not a S3A file system: %s", uri, fs.getClass().getName()));
        }
        this.s3a = (S3AFileSystem)fs;
    }
    
    List<String> parseArgs(final String[] args) {
        return (List<String>)this.commandFormat.parse(args, 1);
    }
    
    private static void printHelp() {
        if (S3GuardTool.cmd == null) {
            System.err.println("Usage: hadoop s3guard [command] [OPTIONS] [s3a://BUCKET]\n\nCommands: \n\tinit - initialize metadata repository\n\tdestroy - destroy metadata repository\n\timport - import metadata from existing S3 data\n\tdiff - report on delta between S3 and repository\n\tprune - truncate older metadata from repository\n");
            System.err.println("\tperform metadata store administrative commands for s3a filesystem.");
        }
        else {
            System.err.println("Usage: hadoop " + S3GuardTool.cmd.getUsage());
        }
        System.err.println();
        System.err.println("When possible and not overridden by more specific options, metadata\nrepository information will be inferred from the S3A URL (if provided)\n\nGeneric options supported are:\n  -conf <config file> - specify an application configuration file\n  -D <property=value> - define a value for a given property\n");
    }
    
    public static int run(final String[] args, final Configuration conf) throws Exception {
        final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length == 0) {
            printHelp();
            return 1;
        }
        final String s;
        final String subCommand = s = otherArgs[0];
        switch (s) {
            case "init": {
                S3GuardTool.cmd = new Init(conf);
                break;
            }
            case "destroy": {
                S3GuardTool.cmd = new Destroy(conf);
                break;
            }
            case "import": {
                S3GuardTool.cmd = new Import(conf);
                break;
            }
            case "diff": {
                S3GuardTool.cmd = new Diff(conf);
                break;
            }
            case "prune": {
                S3GuardTool.cmd = new Prune(conf);
                break;
            }
            default: {
                printHelp();
                return 1;
            }
        }
        return ToolRunner.run(conf, (Tool)S3GuardTool.cmd, otherArgs);
    }
    
    public static void main(final String[] args) throws Exception {
        try {
            final int ret = run(args, new Configuration());
            System.exit(ret);
        }
        catch (CommandFormat.UnknownOptionException e) {
            System.err.println(e.getMessage());
            printHelp();
            System.exit(1);
        }
        catch (Exception e2) {
            e2.printStackTrace(System.err);
            System.exit(99);
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3GuardTool.class);
    }
    
    static class Init extends S3GuardTool
    {
        private static final String NAME = "init";
        public static final String PURPOSE = "initialize metadata repository";
        private static final String USAGE = "init [OPTIONS] [s3a://BUCKET]\n\tinitialize metadata repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n  -read UNIT - Provisioned read throughput units\n  -write UNIT - Provisioned write through put units\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        
        Init(final Configuration conf) {
            super(conf);
            this.commandFormat.addOptionWithValue("read");
            this.commandFormat.addOptionWithValue("write");
        }
        
        @Override
        String getName() {
            return "init";
        }
        
        @Override
        public String getUsage() {
            return "init [OPTIONS] [s3a://BUCKET]\n\tinitialize metadata repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n  -read UNIT - Provisioned read throughput units\n  -write UNIT - Provisioned write through put units\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        }
        
        public int run(final String[] args) throws IOException {
            final List<String> paths = this.parseArgs(args);
            final String readCap = this.commandFormat.getOptValue("read");
            if (readCap != null && !readCap.isEmpty()) {
                final int readCapacity = Integer.parseInt(readCap);
                this.getConf().setInt("fs.s3a.s3guard.ddb.table.capacity.read", readCapacity);
            }
            final String writeCap = this.commandFormat.getOptValue("write");
            if (writeCap != null && !writeCap.isEmpty()) {
                final int writeCapacity = Integer.parseInt(writeCap);
                this.getConf().setInt("fs.s3a.s3guard.ddb.table.capacity.write", writeCapacity);
            }
            if (!this.parseDynamoDBRegion(paths)) {
                System.err.println("init [OPTIONS] [s3a://BUCKET]\n\tinitialize metadata repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n  -read UNIT - Provisioned read throughput units\n  -write UNIT - Provisioned write through put units\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.");
                return 1;
            }
            this.initMetadataStore(true);
            return 0;
        }
    }
    
    static class Destroy extends S3GuardTool
    {
        private static final String NAME = "destroy";
        public static final String PURPOSE = "destroy metadata repository";
        private static final String USAGE = "destroy [OPTIONS] [s3a://BUCKET]\n\tdestroy metadata repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        
        Destroy(final Configuration conf) {
            super(conf);
        }
        
        @Override
        String getName() {
            return "destroy";
        }
        
        @Override
        public String getUsage() {
            return "destroy [OPTIONS] [s3a://BUCKET]\n\tdestroy metadata repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        }
        
        public int run(final String[] args) throws IOException {
            final List<String> paths = this.parseArgs(args);
            if (!this.parseDynamoDBRegion(paths)) {
                System.err.println("destroy [OPTIONS] [s3a://BUCKET]\n\tdestroy metadata repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.");
                return 1;
            }
            this.initMetadataStore(false);
            Preconditions.checkState(this.ms != null, (Object)"Metadata store is not initialized");
            this.ms.destroy();
            S3GuardTool.LOG.info("Metadata store is deleted.");
            return 0;
        }
    }
    
    static class Import extends S3GuardTool
    {
        private static final String NAME = "import";
        public static final String PURPOSE = "import metadata from existing S3 data";
        private static final String USAGE = "import [OPTIONS] [s3a://BUCKET]\n\timport metadata from existing S3 data\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        private final Set<Path> dirCache;
        
        Import(final Configuration conf) {
            super(conf);
            this.dirCache = new HashSet<Path>();
        }
        
        @VisibleForTesting
        void setMetadataStore(final MetadataStore ms) {
            this.ms = ms;
        }
        
        @Override
        String getName() {
            return "import";
        }
        
        @Override
        public String getUsage() {
            return "import [OPTIONS] [s3a://BUCKET]\n\timport metadata from existing S3 data\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        }
        
        private void putParentsIfNotPresent(final FileStatus f) throws IOException {
            Preconditions.checkNotNull((Object)f);
            for (Path parent = f.getPath().getParent(); parent != null; parent = parent.getParent()) {
                if (this.dirCache.contains(parent)) {
                    return;
                }
                final FileStatus dir = DynamoDBMetadataStore.makeDirStatus(parent, f.getOwner());
                this.ms.put(new PathMetadata(dir));
                this.dirCache.add(parent);
            }
        }
        
        private long importDir(final FileStatus status) throws IOException {
            Preconditions.checkArgument(status.isDirectory());
            final RemoteIterator<LocatedFileStatus> it = this.s3a.listFilesAndEmptyDirectories(status.getPath(), true);
            long items = 0L;
            while (it.hasNext()) {
                final LocatedFileStatus located = (LocatedFileStatus)it.next();
                FileStatus child;
                if (located.isDirectory()) {
                    child = DynamoDBMetadataStore.makeDirStatus(located.getPath(), located.getOwner());
                    this.dirCache.add(child.getPath());
                }
                else {
                    child = new S3AFileStatus(located.getLen(), located.getModificationTime(), located.getPath(), located.getBlockSize(), located.getOwner());
                }
                this.putParentsIfNotPresent(child);
                this.ms.put(new PathMetadata(child));
                ++items;
            }
            return items;
        }
        
        public int run(final String[] args) throws IOException {
            final List<String> paths = this.parseArgs(args);
            if (paths.isEmpty()) {
                System.err.println(this.getUsage());
                return 1;
            }
            final String s3Path = paths.get(0);
            this.initS3AFileSystem(s3Path);
            URI uri;
            try {
                uri = new URI(s3Path);
            }
            catch (URISyntaxException e) {
                throw new IOException(e);
            }
            String filePath = uri.getPath();
            if (filePath.isEmpty()) {
                filePath = "/";
            }
            final Path path = new Path(filePath);
            final FileStatus status = this.s3a.getFileStatus(path);
            this.initMetadataStore(false);
            long items = 1L;
            if (status.isFile()) {
                final PathMetadata meta = new PathMetadata(status);
                this.ms.put(meta);
            }
            else {
                items = this.importDir(status);
            }
            System.out.printf("Inserted %d items into Metadata Store%n", items);
            return 0;
        }
    }
    
    static class Diff extends S3GuardTool
    {
        private static final String NAME = "diff";
        public static final String PURPOSE = "report on delta between S3 and repository";
        private static final String USAGE = "diff [OPTIONS] s3a://BUCKET\n\treport on delta between S3 and repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        private static final String SEP = "\t";
        static final String S3_PREFIX = "S3";
        static final String MS_PREFIX = "MS";
        
        Diff(final Configuration conf) {
            super(conf);
        }
        
        @VisibleForTesting
        void setMetadataStore(final MetadataStore ms) {
            Preconditions.checkNotNull((Object)ms);
            this.ms = ms;
        }
        
        @Override
        String getName() {
            return "diff";
        }
        
        @Override
        public String getUsage() {
            return "diff [OPTIONS] s3a://BUCKET\n\treport on delta between S3 and repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        }
        
        private static String formatFileStatus(final FileStatus status) {
            return String.format("%s%s%d%s%s", status.isDirectory() ? "D" : "F", "\t", status.getLen(), "\t", status.getPath().toString());
        }
        
        private static boolean differ(final FileStatus thisOne, final FileStatus thatOne) {
            Preconditions.checkArgument(thisOne != null || thatOne != null);
            return thisOne == null || thatOne == null || thisOne.getLen() != thatOne.getLen() || thisOne.isDirectory() != thatOne.isDirectory() || (!thisOne.isDirectory() && thisOne.getModificationTime() != thatOne.getModificationTime());
        }
        
        private static void printDiff(final FileStatus msStatus, final FileStatus s3Status, final PrintStream out) {
            Preconditions.checkArgument(msStatus != null || s3Status != null);
            if (msStatus != null && s3Status != null) {
                Preconditions.checkArgument(msStatus.getPath().equals((Object)s3Status.getPath()), (Object)String.format("The path from metadata store and s3 are different: ms=%s s3=%s", msStatus.getPath(), s3Status.getPath()));
            }
            if (differ(msStatus, s3Status)) {
                if (s3Status != null) {
                    out.printf("%s%s%s%n", "S3", "\t", formatFileStatus(s3Status));
                }
                if (msStatus != null) {
                    out.printf("%s%s%s%n", "MS", "\t", formatFileStatus(msStatus));
                }
            }
        }
        
        private void compareDir(final FileStatus msDir, final FileStatus s3Dir, final PrintStream out) throws IOException {
            Preconditions.checkArgument(msDir != null || s3Dir != null);
            if (msDir != null && s3Dir != null) {
                Preconditions.checkArgument(msDir.getPath().equals((Object)s3Dir.getPath()), (Object)String.format("The path from metadata store and s3 are different: ms=%s s3=%s", msDir.getPath(), s3Dir.getPath()));
            }
            final Map<Path, FileStatus> s3Children = new HashMap<Path, FileStatus>();
            if (s3Dir != null && s3Dir.isDirectory()) {
                for (final FileStatus status : this.s3a.listStatus(s3Dir.getPath())) {
                    s3Children.put(status.getPath(), status);
                }
            }
            final Map<Path, FileStatus> msChildren = new HashMap<Path, FileStatus>();
            if (msDir != null && msDir.isDirectory()) {
                final DirListingMetadata dirMeta = this.ms.listChildren(msDir.getPath());
                if (dirMeta != null) {
                    for (final PathMetadata meta : dirMeta.getListing()) {
                        final FileStatus status2 = meta.getFileStatus();
                        msChildren.put(status2.getPath(), status2);
                    }
                }
            }
            final Set<Path> allPaths = new HashSet<Path>(s3Children.keySet());
            allPaths.addAll(msChildren.keySet());
            for (final Path path : allPaths) {
                final FileStatus s3Status = s3Children.get(path);
                final FileStatus msStatus = msChildren.get(path);
                printDiff(msStatus, s3Status, out);
                if ((s3Status != null && s3Status.isDirectory()) || (msStatus != null && msStatus.isDirectory())) {
                    this.compareDir(msStatus, s3Status, out);
                }
            }
            out.flush();
        }
        
        private void compareRoot(final Path path, final PrintStream out) throws IOException {
            final Path qualified = this.s3a.qualify(path);
            FileStatus s3Status = null;
            try {
                s3Status = this.s3a.getFileStatus(qualified);
            }
            catch (FileNotFoundException ex) {}
            final PathMetadata meta = this.ms.get(qualified);
            final FileStatus msStatus = (meta != null && !meta.isDeleted()) ? meta.getFileStatus() : null;
            this.compareDir(msStatus, s3Status, out);
        }
        
        @VisibleForTesting
        public int run(final String[] args, final PrintStream out) throws IOException {
            final List<String> paths = this.parseArgs(args);
            if (paths.isEmpty()) {
                out.println("diff [OPTIONS] s3a://BUCKET\n\treport on delta between S3 and repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.");
                return 1;
            }
            final String s3Path = paths.get(0);
            this.initS3AFileSystem(s3Path);
            this.initMetadataStore(true);
            URI uri;
            try {
                uri = new URI(s3Path);
            }
            catch (URISyntaxException e) {
                throw new IOException(e);
            }
            Path root;
            if (uri.getPath().isEmpty()) {
                root = new Path("/");
            }
            else {
                root = new Path(uri.getPath());
            }
            root = this.s3a.qualify(root);
            this.compareRoot(root, out);
            out.flush();
            return 0;
        }
        
        public int run(final String[] args) throws IOException {
            return this.run(args, System.out);
        }
    }
    
    static class Prune extends S3GuardTool
    {
        private static final String NAME = "prune";
        public static final String PURPOSE = "truncate older metadata from repository";
        private static final String USAGE = "prune [OPTIONS] [s3a://BUCKET]\n\ttruncate older metadata from repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        
        Prune(final Configuration conf) {
            super(conf);
            this.commandFormat.addOptionWithValue("days");
            this.commandFormat.addOptionWithValue("hours");
            this.commandFormat.addOptionWithValue("minutes");
            this.commandFormat.addOptionWithValue("seconds");
        }
        
        @VisibleForTesting
        void setMetadataStore(final MetadataStore ms) {
            Preconditions.checkNotNull((Object)ms);
            this.ms = ms;
        }
        
        @Override
        String getName() {
            return "prune";
        }
        
        @Override
        public String getUsage() {
            return "prune [OPTIONS] [s3a://BUCKET]\n\ttruncate older metadata from repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.";
        }
        
        private long getDeltaComponent(final TimeUnit unit, final String arg) {
            final String raw = this.commandFormat.getOptValue(arg);
            if (raw == null || raw.isEmpty()) {
                return 0L;
            }
            final Long parsed = Long.parseLong(raw);
            return unit.toMillis(parsed);
        }
        
        @VisibleForTesting
        public int run(final String[] args, final PrintStream out) throws InterruptedException, IOException {
            final List<String> paths = this.parseArgs(args);
            if (!this.parseDynamoDBRegion(paths)) {
                System.err.println("prune [OPTIONS] [s3a://BUCKET]\n\ttruncate older metadata from repository\n\nCommon options:\n  -meta URL - Metadata repository details (implementation-specific)\n\nAmazon DynamoDB-specific options:\n  -region REGION - Service region for connections\n\n  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n  Specifying both the -region option and an S3A path\n  is not supported.");
                return 1;
            }
            this.initMetadataStore(false);
            final Configuration conf = this.getConf();
            final long confDelta = conf.getLong("fs.s3a.s3guard.cli.prune.age", 0L);
            long cliDelta = 0L;
            cliDelta += this.getDeltaComponent(TimeUnit.DAYS, "days");
            cliDelta += this.getDeltaComponent(TimeUnit.HOURS, "hours");
            cliDelta += this.getDeltaComponent(TimeUnit.MINUTES, "minutes");
            cliDelta += this.getDeltaComponent(TimeUnit.SECONDS, "seconds");
            if (confDelta <= 0L && cliDelta <= 0L) {
                System.err.println("You must specify a positive age for metadata to prune.");
            }
            long delta = confDelta;
            if (cliDelta > 0L) {
                delta = cliDelta;
            }
            final long now = System.currentTimeMillis();
            final long divide = now - delta;
            this.ms.prune(divide);
            out.flush();
            return 0;
        }
        
        public int run(final String[] args) throws InterruptedException, IOException {
            return this.run(args, System.out);
        }
    }
}
