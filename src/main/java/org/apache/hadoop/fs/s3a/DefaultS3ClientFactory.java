// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.S3ClientOptions;
import org.apache.hadoop.util.VersionInfo;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import java.io.IOException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import com.amazonaws.services.s3.AmazonS3;
import java.net.URI;
import org.slf4j.Logger;
import org.apache.hadoop.conf.Configured;

public class DefaultS3ClientFactory extends Configured implements S3ClientFactory
{
    private static final Logger LOG;
    
    public AmazonS3 createS3Client(final URI name) throws IOException {
        final Configuration conf = this.getConf();
        final AWSCredentialsProvider credentials = (AWSCredentialsProvider)S3AUtils.createAWSCredentialProviderSet(name, conf);
        final ClientConfiguration awsConf = createAwsConf(this.getConf());
        final AmazonS3 s3 = this.newAmazonS3Client(credentials, awsConf);
        return createAmazonS3Client(s3, conf, credentials, awsConf);
    }
    
    public static ClientConfiguration createAwsConf(final Configuration conf) {
        final ClientConfiguration awsConf = new ClientConfiguration();
        initConnectionSettings(conf, awsConf);
        initProxySupport(conf, awsConf);
        initUserAgent(conf, awsConf);
        return awsConf;
    }
    
    protected AmazonS3 newAmazonS3Client(final AWSCredentialsProvider credentials, final ClientConfiguration awsConf) {
        return (AmazonS3)new AmazonS3Client(credentials, awsConf);
    }
    
    private static void initConnectionSettings(final Configuration conf, final ClientConfiguration awsConf) {
        awsConf.setMaxConnections(S3AUtils.intOption(conf, "fs.s3a.connection.maximum", 15, 1));
        final boolean secureConnections = conf.getBoolean("fs.s3a.connection.ssl.enabled", true);
        awsConf.setProtocol(secureConnections ? Protocol.HTTPS : Protocol.HTTP);
        awsConf.setMaxErrorRetry(S3AUtils.intOption(conf, "fs.s3a.attempts.maximum", 20, 0));
        awsConf.setConnectionTimeout(S3AUtils.intOption(conf, "fs.s3a.connection.establish.timeout", 50000, 0));
        awsConf.setSocketTimeout(S3AUtils.intOption(conf, "fs.s3a.connection.timeout", 200000, 0));
        final int sockSendBuffer = S3AUtils.intOption(conf, "fs.s3a.socket.send.buffer", 8192, 2048);
        final int sockRecvBuffer = S3AUtils.intOption(conf, "fs.s3a.socket.recv.buffer", 8192, 2048);
        awsConf.setSocketBufferSizeHints(sockSendBuffer, sockRecvBuffer);
        final String signerOverride = conf.getTrimmed("fs.s3a.signing-algorithm", "");
        if (!signerOverride.isEmpty()) {
            DefaultS3ClientFactory.LOG.debug("Signer override = {}", (Object)signerOverride);
            awsConf.setSignerOverride(signerOverride);
        }
    }
    
    private static void initProxySupport(final Configuration conf, final ClientConfiguration awsConf) throws IllegalArgumentException {
        final String proxyHost = conf.getTrimmed("fs.s3a.proxy.host", "");
        final int proxyPort = conf.getInt("fs.s3a.proxy.port", -1);
        if (!proxyHost.isEmpty()) {
            awsConf.setProxyHost(proxyHost);
            if (proxyPort >= 0) {
                awsConf.setProxyPort(proxyPort);
            }
            else if (conf.getBoolean("fs.s3a.connection.ssl.enabled", true)) {
                DefaultS3ClientFactory.LOG.warn("Proxy host set without port. Using HTTPS default 443");
                awsConf.setProxyPort(443);
            }
            else {
                DefaultS3ClientFactory.LOG.warn("Proxy host set without port. Using HTTP default 80");
                awsConf.setProxyPort(80);
            }
            final String proxyUsername = conf.getTrimmed("fs.s3a.proxy.username");
            final String proxyPassword = conf.getTrimmed("fs.s3a.proxy.password");
            if (proxyUsername == null != (proxyPassword == null)) {
                final String msg = "Proxy error: fs.s3a.proxy.username or fs.s3a.proxy.password set without the other.";
                DefaultS3ClientFactory.LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }
            awsConf.setProxyUsername(proxyUsername);
            awsConf.setProxyPassword(proxyPassword);
            awsConf.setProxyDomain(conf.getTrimmed("fs.s3a.proxy.domain"));
            awsConf.setProxyWorkstation(conf.getTrimmed("fs.s3a.proxy.workstation"));
            if (DefaultS3ClientFactory.LOG.isDebugEnabled()) {
                DefaultS3ClientFactory.LOG.debug("Using proxy server {}:{} as user {} with password {} on domain {} as workstation {}", new Object[] { awsConf.getProxyHost(), awsConf.getProxyPort(), String.valueOf(awsConf.getProxyUsername()), awsConf.getProxyPassword(), awsConf.getProxyDomain(), awsConf.getProxyWorkstation() });
            }
        }
        else if (proxyPort >= 0) {
            final String msg2 = "Proxy error: fs.s3a.proxy.port set without fs.s3a.proxy.host";
            DefaultS3ClientFactory.LOG.error(msg2);
            throw new IllegalArgumentException(msg2);
        }
    }
    
    private static void initUserAgent(final Configuration conf, final ClientConfiguration awsConf) {
        String userAgent = "Hadoop " + VersionInfo.getVersion();
        final String userAgentPrefix = conf.getTrimmed("fs.s3a.user.agent.prefix", "");
        if (!userAgentPrefix.isEmpty()) {
            userAgent = userAgentPrefix + ", " + userAgent;
        }
        DefaultS3ClientFactory.LOG.debug("Using User-Agent: {}", (Object)userAgent);
        awsConf.setUserAgent(userAgent);
    }
    
    private static AmazonS3 createAmazonS3Client(final AmazonS3 s3, final Configuration conf, final AWSCredentialsProvider credentials, final ClientConfiguration awsConf) throws IllegalArgumentException {
        final String endPoint = conf.getTrimmed("fs.s3a.endpoint", "");
        if (!endPoint.isEmpty()) {
            try {
                s3.setEndpoint(endPoint);
            }
            catch (IllegalArgumentException e) {
                final String msg = "Incorrect endpoint: " + e.getMessage();
                DefaultS3ClientFactory.LOG.error(msg);
                throw new IllegalArgumentException(msg, e);
            }
        }
        enablePathStyleAccessIfRequired(s3, conf);
        return s3;
    }
    
    private static void enablePathStyleAccessIfRequired(final AmazonS3 s3, final Configuration conf) {
        final boolean pathStyleAccess = conf.getBoolean("fs.s3a.path.style.access", false);
        if (pathStyleAccess) {
            DefaultS3ClientFactory.LOG.debug("Enabling path style access!");
            s3.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        }
    }
    
    static {
        LOG = S3AFileSystem.LOG;
    }
}
