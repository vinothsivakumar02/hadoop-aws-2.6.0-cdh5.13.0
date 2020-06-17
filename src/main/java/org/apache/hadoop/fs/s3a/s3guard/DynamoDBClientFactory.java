// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import com.amazonaws.regions.Regions;
import org.apache.commons.lang.StringUtils;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import java.net.URI;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.conf.Configured;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;

@InterfaceAudience.Private
public interface DynamoDBClientFactory extends Configurable
{
    public static final Logger LOG = LoggerFactory.getLogger((Class)DynamoDBClientFactory.class);
    
    AmazonDynamoDB createDynamoDBClient(final String p0) throws IOException;
    
    public static class DefaultDynamoDBClientFactory extends Configured implements DynamoDBClientFactory
    {
        public AmazonDynamoDB createDynamoDBClient(final String defaultRegion) throws IOException {
            assert this.getConf() != null : "Should have been configured before usage";
            final Configuration conf = this.getConf();
            final AWSCredentialsProvider credentials = (AWSCredentialsProvider)S3AUtils.createAWSCredentialProviderSet(null, conf);
            final ClientConfiguration awsConf = DefaultS3ClientFactory.createAwsConf(conf);
            final String region = getRegion(conf, defaultRegion);
            DefaultDynamoDBClientFactory.LOG.debug("Creating DynamoDB client in region {}", (Object)region);
            return (AmazonDynamoDB)((AmazonDynamoDBClientBuilder)((AmazonDynamoDBClientBuilder)((AmazonDynamoDBClientBuilder)AmazonDynamoDBClientBuilder.standard().withCredentials(credentials)).withClientConfiguration(awsConf)).withRegion(region)).build();
        }
        
        static String getRegion(final Configuration conf, final String defaultRegion) throws IOException {
            String region = conf.getTrimmed("fs.s3a.s3guard.ddb.region");
            if (StringUtils.isEmpty(region)) {
                region = defaultRegion;
            }
            try {
                Regions.fromName(region);
            }
            catch (IllegalArgumentException | NullPointerException ex2) {
                throw new IOException("Invalid region specified: " + region + "; " + "Region can be configured with " + "fs.s3a.s3guard.ddb.region" + ": " + validRegionsString());
            }
            return region;
        }
        
        private static String validRegionsString() {
            final String DELIMITER = ", ";
            final Regions[] regions = Regions.values();
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < regions.length; ++i) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(regions[i].getName());
            }
            return sb.toString();
        }
    }
}
