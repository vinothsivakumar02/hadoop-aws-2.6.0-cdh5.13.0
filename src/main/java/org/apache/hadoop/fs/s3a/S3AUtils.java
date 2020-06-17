// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.io.Closeable;
import java.util.List;
import java.util.Collection;
import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.ProviderUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import java.util.Date;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.io.InterruptedIOException;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.AmazonServiceException;
import java.io.IOException;
import com.amazonaws.AmazonClientException;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3AUtils
{
    private static final Logger LOG;
    static final String CONSTRUCTOR_EXCEPTION = "constructor exception";
    static final String INSTANTIATION_EXCEPTION = "instantiation exception";
    static final String NOT_AWS_PROVIDER = "does not implement AWSCredentialsProvider";
    static final String ABSTRACT_PROVIDER = "is abstract and therefore cannot be created";
    static final String ENDPOINT_KEY = "Endpoint";
    static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";
    public static final String SSE_C_NO_KEY_ERROR;
    public static final String SSE_S3_WITH_KEY_ERROR;
    
    private S3AUtils() {
    }
    
    public static IOException translateException(final String operation, final Path path, final AmazonClientException exception) {
        return translateException(operation, path.toString(), exception);
    }
    
    public static IOException translateException(final String operation, final String path, final AmazonClientException exception) {
        String message = String.format("%s%s: %s", operation, (path != null) ? (" on " + path) : "", exception);
        if (exception instanceof AmazonServiceException) {
            final AmazonServiceException ase = (AmazonServiceException)exception;
            final AmazonS3Exception s3Exception = (ase instanceof AmazonS3Exception) ? (AmazonS3Exception) ase : null;
            final int status = ase.getStatusCode();
            IOException ioe = null;
            switch (status) {
                case 301: {
                    if (s3Exception != null) {
                        if (s3Exception.getAdditionalDetails() != null && s3Exception.getAdditionalDetails().containsKey("Endpoint")) {
                            message = String.format("Received permanent redirect response to endpoint %s.  This likely indicates that the S3 endpoint configured in %s does not match the AWS region containing the bucket.", s3Exception.getAdditionalDetails().get("Endpoint"), "fs.s3a.endpoint");
                        }
                        ioe = new AWSS3IOException(message, s3Exception);
                        break;
                    }
                    ioe = new AWSServiceIOException(message, ase);
                    break;
                }
                case 401:
                case 403: {
                    ioe = new AccessDeniedException(path, null, message);
                    ioe.initCause((Throwable)ase);
                    break;
                }
                case 404:
                case 410: {
                    ioe = new FileNotFoundException(message);
                    ioe.initCause((Throwable)ase);
                    break;
                }
                case 416: {
                    ioe = new EOFException(message);
                    break;
                }
                default: {
                    ioe = ((s3Exception != null) ? new AWSS3IOException(message, s3Exception) : new AWSServiceIOException(message, ase));
                    break;
                }
            }
            return ioe;
        }
        if (containsInterruptedException((Throwable)exception)) {
            return (IOException)new InterruptedIOException(message).initCause((Throwable)exception);
        }
        return new AWSClientIOException(message, exception);
    }
    
    public static IOException extractException(final String operation, final String path, final ExecutionException ee) {
        final Throwable cause = ee.getCause();
        IOException ioe;
        if (cause instanceof AmazonClientException) {
            ioe = translateException(operation, path, (AmazonClientException)cause);
        }
        else if (cause instanceof IOException) {
            ioe = (IOException)cause;
        }
        else {
            ioe = new IOException(operation + " failed: " + cause, cause);
        }
        return ioe;
    }
    
    static boolean containsInterruptedException(final Throwable thrown) {
        return thrown != null && (thrown instanceof InterruptedException || thrown instanceof InterruptedIOException || containsInterruptedException(thrown.getCause()));
    }
    
    public static String stringify(final AmazonServiceException e) {
        final StringBuilder builder = new StringBuilder(String.format("%s: %s error %d: %s; %s%s%n", e.getErrorType(), e.getServiceName(), e.getStatusCode(), e.getErrorCode(), e.getErrorMessage(), e.isRetryable() ? " (retryable)" : ""));
        final String rawResponseContent = e.getRawResponseContent();
        if (rawResponseContent != null) {
            builder.append(rawResponseContent);
        }
        return builder.toString();
    }
    
    public static String stringify(final AmazonS3Exception e) {
        final StringBuilder builder = new StringBuilder(stringify((AmazonServiceException)e));
        final Map<String, String> details = (Map<String, String>)e.getAdditionalDetails();
        if (details != null) {
            builder.append('\n');
            for (final Map.Entry<String, String> d : details.entrySet()) {
                builder.append(d.getKey()).append('=').append(d.getValue()).append('\n');
            }
        }
        return builder.toString();
    }
    
    public static S3AFileStatus createFileStatus(final Path keyPath, final S3ObjectSummary summary, final long blockSize, final String owner) {
        final long size = summary.getSize();
        return createFileStatus(keyPath, objectRepresentsDirectory(summary.getKey(), size), size, summary.getLastModified(), blockSize, owner);
    }
    
    public static S3AFileStatus createUploadFileStatus(final Path keyPath, final boolean isDir, final long size, final long blockSize, final String owner) {
        final Date date = isDir ? null : new Date();
        return createFileStatus(keyPath, isDir, size, date, blockSize, owner);
    }
    
    private static S3AFileStatus createFileStatus(final Path keyPath, final boolean isDir, final long size, final Date modified, final long blockSize, final String owner) {
        if (isDir) {
            return new S3AFileStatus(Tristate.UNKNOWN, keyPath, owner);
        }
        return new S3AFileStatus(size, dateToLong(modified), keyPath, blockSize, owner);
    }
    
    public static boolean objectRepresentsDirectory(final String name, final long size) {
        return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
    }
    
    public static long dateToLong(final Date date) {
        if (date == null) {
            return 0L;
        }
        return date.getTime();
    }
    
    public static AWSCredentialProviderList createAWSCredentialProviderSet(final URI binding, final Configuration conf) throws IOException {
        final AWSCredentialProviderList credentials = new AWSCredentialProviderList();
        Class<?>[] awsClasses;
        try {
            awsClasses = (Class<?>[])conf.getClasses("fs.s3a.aws.credentials.provider", new Class[0]);
        }
        catch (RuntimeException e) {
            final Throwable c = (e.getCause() != null) ? e.getCause() : e;
            throw new IOException("From option fs.s3a.aws.credentials.provider " + c, c);
        }
        if (awsClasses.length == 0) {
            final S3xLoginHelper.Login creds = getAWSAccessKeys(binding, conf);
            credentials.add((AWSCredentialsProvider)new BasicAWSCredentialsProvider(creds.getUser(), creds.getPassword()));
            credentials.add((AWSCredentialsProvider)new EnvironmentVariableCredentialsProvider());
            credentials.add((AWSCredentialsProvider)SharedInstanceProfileCredentialsProvider.getInstance());
        }
        else {
            for (Class<?> aClass : awsClasses) {
                if (aClass == InstanceProfileCredentialsProvider.class) {
                    S3AUtils.LOG.debug("Found {}, but will use {} instead.", (Object)aClass.getName(), (Object)SharedInstanceProfileCredentialsProvider.class.getName());
                    aClass = SharedInstanceProfileCredentialsProvider.class;
                }
                credentials.add(createAWSCredentialProvider(conf, aClass));
            }
        }
        S3AUtils.LOG.debug("For URI {}, using credentials {}", (Object)S3xLoginHelper.toString(binding), (Object)credentials);
        return credentials;
    }
    
    static AWSCredentialsProvider createAWSCredentialProvider(final Configuration conf, final Class<?> credClass) throws IOException {
        AWSCredentialsProvider credentials = null;
        final String className = credClass.getName();
        if (!AWSCredentialsProvider.class.isAssignableFrom(credClass)) {
            throw new IOException("Class " + credClass + " " + "does not implement AWSCredentialsProvider");
        }
        if (Modifier.isAbstract(credClass.getModifiers())) {
            throw new IOException("Class " + credClass + " " + "is abstract and therefore cannot be created");
        }
        S3AUtils.LOG.debug("Credential provider class is {}", (Object)className);
        try {
            Constructor cons = getConstructor(credClass, Configuration.class);
            if (cons != null) {
                credentials = (AWSCredentialsProvider) cons.newInstance(conf);
                return credentials;
            }
            final Method factory = getFactoryMethod(credClass, AWSCredentialsProvider.class, "getInstance");
            if (factory != null) {
                credentials = (AWSCredentialsProvider)factory.invoke(null, new Object[0]);
                return credentials;
            }
            cons = getConstructor(credClass, (Class<?>[])new Class[0]);
            if (cons != null) {
                credentials = (AWSCredentialsProvider) cons.newInstance(new Object[0]);
                return credentials;
            }
            throw new IOException(String.format("%s constructor exception.  A class specified in %s must provide a public constructor accepting Configuration, or a public factory method named getInstance that accepts no arguments, or a public default constructor.", className, "fs.s3a.aws.credentials.provider"));
        }
        catch (ReflectiveOperationException | IllegalArgumentException e) {
            throw new IOException(className + " " + "instantiation exception" + ".", e);
        }
    }
    
    public static S3xLoginHelper.Login getAWSAccessKeys(final URI name, final Configuration conf) throws IOException {
        final S3xLoginHelper.Login login = S3xLoginHelper.extractLoginDetailsWithWarnings(name);
        final Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(conf, (Class)S3AFileSystem.class);
        final String accessKey = getPasswordLegacy(c, "fs.s3a.access.key", login.getUser());
        final String secretKey = getPasswordLegacy(c, "fs.s3a.secret.key", login.getPassword());
        return new S3xLoginHelper.Login(accessKey, secretKey);
    }
    
    private static String getPasswordLegacy(final Configuration conf, final String key, final String val) throws IOException {
        final String v = getPassword(conf, key, val);
        if (v.equals("")) {
            if (key.equals("fs.s3a.access.key")) {
                return getPassword(conf, "fs.s3a.awsAccessKeyId", val);
            }
            if (key.equals("fs.s3a.secret.key")) {
                return getPassword(conf, "fs.s3a.awsSecretAccessKey", val);
            }
        }
        return v;
    }
    
    static String getPassword(final Configuration conf, final String key, final String val) throws IOException {
        final String defVal = "";
        return getPassword(conf, key, val, defVal);
    }
    
    private static String getPassword(final Configuration conf, final String key, final String val, final String defVal) throws IOException {
        return StringUtils.isEmpty(val) ? lookupPassword(conf, key, defVal) : val;
    }
    
    static String lookupPassword(final Configuration conf, final String key, final String defVal) throws IOException {
        try {
            final char[] pass = conf.getPassword(key);
            return (pass != null) ? new String(pass).trim() : defVal;
        }
        catch (IOException ioe) {
            throw new IOException("Cannot find password option " + key, ioe);
        }
    }
    
    public static String stringify(final S3ObjectSummary summary) {
        final StringBuilder builder = new StringBuilder(summary.getKey().length() + 100);
        builder.append(summary.getKey()).append(' ');
        builder.append("size=").append(summary.getSize());
        return builder.toString();
    }
    
    static int intOption(final Configuration conf, final String key, final int defVal, final int min) {
        final int v = conf.getInt(key, defVal);
        Preconditions.checkArgument(v >= min, (Object)String.format("Value of %s: %d is below the minimum value %d", key, v, min));
        S3AUtils.LOG.debug("Value of {} is {}", (Object)key, (Object)v);
        return v;
    }
    
    static long longOption(final Configuration conf, final String key, final long defVal, final long min) {
        final long v = conf.getLong(key, defVal);
        Preconditions.checkArgument(v >= min, (Object)String.format("Value of %s: %d is below the minimum value %d", key, v, min));
        S3AUtils.LOG.debug("Value of {} is {}", (Object)key, (Object)v);
        return v;
    }
    
    static long longBytesOption(final Configuration conf, final String key, final long defVal, final long min) {
        final long v = conf.getLongBytes(key, defVal);
        Preconditions.checkArgument(v >= min, (Object)String.format("Value of %s: %d is below the minimum value %d", key, v, min));
        S3AUtils.LOG.debug("Value of {} is {}", (Object)key, (Object)v);
        return v;
    }
    
    public static long getMultipartSizeProperty(final Configuration conf, final String property, final long defVal) {
        long partSize = conf.getLongBytes(property, defVal);
        if (partSize < 5242880L) {
            S3AUtils.LOG.warn("{} must be at least 5 MB; configured value is {}", (Object)property, (Object)partSize);
            partSize = 5242880L;
        }
        return partSize;
    }
    
    public static int ensureOutputParameterInRange(final String name, final long size) {
        if (size > 2147483647L) {
            S3AUtils.LOG.warn("s3a: {} capped to ~2.14GB (maximum allowed size with current output mechanism)", (Object)name);
            return Integer.MAX_VALUE;
        }
        return (int)size;
    }
    
    private static Constructor<?> getConstructor(final Class<?> cl, final Class<?>... args) {
        try {
            final Constructor cons = cl.getDeclaredConstructor(args);
            return (Constructor<?>)(Modifier.isPublic(cons.getModifiers()) ? cons : null);
        }
        catch (NoSuchMethodException | SecurityException ex2) {
            return null;
        }
    }
    
    private static Method getFactoryMethod(final Class<?> cl, final Class<?> returnType, final String methodName) {
        try {
            final Method m = cl.getDeclaredMethod(methodName, (Class<?>[])new Class[0]);
            if (Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers()) && returnType.isAssignableFrom(m.getReturnType())) {
                return m;
            }
            return null;
        }
        catch (NoSuchMethodException | SecurityException ex2) {
            return null;
        }
    }
    
    public static Configuration propagateBucketOptions(final Configuration source, final String bucket) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), (Object)"bucket");
        final String bucketPrefix = "fs.s3a.bucket." + bucket + '.';
        S3AUtils.LOG.debug("Propagating entries under {}", (Object)bucketPrefix);
        final Configuration dest = new Configuration(source);
        for (final Map.Entry<String, String> entry : source) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            if (key.startsWith(bucketPrefix)) {
                if (bucketPrefix.equals(key)) {
                    continue;
                }
                final String stripped = key.substring(bucketPrefix.length());
                if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
                    S3AUtils.LOG.debug("Ignoring bucket option {}", (Object)key);
                }
                else {
                    final String generic = "fs.s3a." + stripped;
                    S3AUtils.LOG.debug("Updating {}", (Object)generic);
                    dest.set(generic, value, key);
                }
            }
        }
        return dest;
    }
    
    static void patchSecurityCredentialProviders(final Configuration conf) {
        final Collection<String> customCredentials = (Collection<String>)conf.getStringCollection("fs.s3a.security.credential.provider.path");
        final Collection<String> hadoopCredentials = (Collection<String>)conf.getStringCollection("hadoop.security.credential.provider.path");
        if (!customCredentials.isEmpty()) {
            final List<String> all = (List<String>)Lists.newArrayList((Iterable)customCredentials);
            all.addAll(hadoopCredentials);
            final String joined = StringUtils.join((Collection)all, ',');
            S3AUtils.LOG.debug("Setting {} to {}", (Object)"hadoop.security.credential.provider.path", (Object)joined);
            conf.set("hadoop.security.credential.provider.path", joined, "patch of fs.s3a.security.credential.provider.path");
        }
    }
    
    static String getServerSideEncryptionKey(final Configuration conf) {
        try {
            return lookupPassword(conf, "fs.s3a.server-side-encryption.key", getPassword(conf, "fs.s3a.server-side-encryption-key", null, null));
        }
        catch (IOException e) {
            S3AUtils.LOG.error("Cannot retrieve SERVER_SIDE_ENCRYPTION_KEY", (Throwable)e);
            return "";
        }
    }
    
    static S3AEncryptionMethods getEncryptionAlgorithm(final Configuration conf) throws IOException {
        final S3AEncryptionMethods sse = S3AEncryptionMethods.getMethod(conf.getTrimmed("fs.s3a.server-side-encryption-algorithm"));
        final String sseKey = getServerSideEncryptionKey(conf);
        final int sseKeyLen = StringUtils.isBlank(sseKey) ? 0 : sseKey.length();
        final String diagnostics = passwordDiagnostics(sseKey, "key");
        switch (sse) {
            case SSE_C: {
                if (sseKeyLen == 0) {
                    throw new IOException(S3AUtils.SSE_C_NO_KEY_ERROR);
                }
                break;
            }
            case SSE_S3: {
                if (sseKeyLen != 0) {
                    throw new IOException(S3AUtils.SSE_S3_WITH_KEY_ERROR + " (" + diagnostics + ")");
                }
                break;
            }
            case SSE_KMS: {
                S3AUtils.LOG.debug("Using SSE-KMS with {}", (Object)diagnostics);
                break;
            }
            default: {
                S3AUtils.LOG.debug("Data is unencrypted");
                break;
            }
        }
        S3AUtils.LOG.debug("Using SSE-C with {}", (Object)diagnostics);
        return sse;
    }
    
    private static String passwordDiagnostics(final String pass, final String description) {
        if (pass == null) {
            return "null " + description;
        }
        final int len = pass.length();
        switch (len) {
            case 0: {
                return "empty " + description;
            }
            case 1: {
                return description + " of length 1";
            }
            default: {
                return description + " of length " + len + " ending with " + pass.charAt(len - 1);
            }
        }
    }
    
    public static void closeAll(final Logger log, final Closeable... closeables) {
        for (final Closeable c : closeables) {
            if (c != null) {
                try {
                    if (log != null) {
                        log.debug("Closing {}", (Object)c);
                    }
                    c.close();
                }
                catch (Exception e) {
                    if (log != null && log.isDebugEnabled()) {
                        log.debug("Exception in closing {}", (Object)c, (Object)e);
                    }
                }
            }
        }
    }
    
    static {
        LOG = S3AFileSystem.LOG;
        SSE_C_NO_KEY_ERROR = S3AEncryptionMethods.SSE_C.getMethod() + " is enabled but no encryption key was declared in " + "fs.s3a.server-side-encryption.key";
        SSE_S3_WITH_KEY_ERROR = S3AEncryptionMethods.SSE_S3.getMethod() + " is enabled but an encryption key was set in " + "fs.s3a.server-side-encryption.key";
    }
}
