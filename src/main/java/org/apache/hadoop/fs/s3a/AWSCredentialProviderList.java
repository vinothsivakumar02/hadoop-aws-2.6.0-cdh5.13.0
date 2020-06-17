// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import java.util.Iterator;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import com.amazonaws.auth.AWSCredentialsProvider;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AWSCredentialProviderList implements AWSCredentialsProvider
{
    private static final Logger LOG;
    public static final String NO_AWS_CREDENTIAL_PROVIDERS = "No AWS Credential Providers";
    private final List<AWSCredentialsProvider> providers;
    private boolean reuseLastProvider;
    private AWSCredentialsProvider lastProvider;
    
    public AWSCredentialProviderList() {
        this.providers = new ArrayList<AWSCredentialsProvider>(1);
        this.reuseLastProvider = true;
    }
    
    public AWSCredentialProviderList(final Collection<AWSCredentialsProvider> providers) {
        this.providers = new ArrayList<AWSCredentialsProvider>(1);
        this.reuseLastProvider = true;
        this.providers.addAll(providers);
    }
    
    public void add(final AWSCredentialsProvider p) {
        this.providers.add(p);
    }
    
    public void setReuseLastProvider(final boolean reuseLastProvider) {
        this.reuseLastProvider = reuseLastProvider;
    }
    
    public boolean isReuseLastProvider() {
        return this.reuseLastProvider;
    }
    
    public void refresh() {
        for (final AWSCredentialsProvider provider : this.providers) {
            provider.refresh();
        }
    }
    
    public AWSCredentials getCredentials() {
        this.checkNotEmpty();
        if (this.reuseLastProvider && this.lastProvider != null) {
            return this.lastProvider.getCredentials();
        }
        AmazonClientException lastException = null;
        for (final AWSCredentialsProvider provider : this.providers) {
            try {
                final AWSCredentials credentials = provider.getCredentials();
                if ((credentials.getAWSAccessKeyId() != null && credentials.getAWSSecretKey() != null) || credentials instanceof AnonymousAWSCredentials) {
                    this.lastProvider = provider;
                    AWSCredentialProviderList.LOG.debug("Using credentials from {}", (Object)provider);
                    return credentials;
                }
                continue;
            }
            catch (AmazonClientException e) {
                lastException = e;
                AWSCredentialProviderList.LOG.debug("No credentials provided by {}: {}", new Object[] { provider, e.toString(), e });
            }
        }
        String message = "No AWS Credentials provided by " + this.listProviderNames();
        if (lastException != null) {
            message = message + ": " + lastException;
        }
        throw new AmazonClientException(message, (Throwable)lastException);
    }
    
    @VisibleForTesting
    List<AWSCredentialsProvider> getProviders() {
        return this.providers;
    }
    
    public void checkNotEmpty() {
        if (this.providers.isEmpty()) {
            throw new AmazonClientException("No AWS Credential Providers");
        }
    }
    
    public String listProviderNames() {
        final StringBuilder sb = new StringBuilder(this.providers.size() * 32);
        for (final AWSCredentialsProvider provider : this.providers) {
            sb.append(provider.getClass().getSimpleName());
            sb.append(' ');
        }
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return "AWSCredentialProviderList: " + StringUtils.join((Collection)this.providers, " ");
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)AWSCredentialProviderList.class);
    }
}
