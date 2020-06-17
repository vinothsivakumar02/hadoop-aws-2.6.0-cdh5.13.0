// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3native;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.net.URISyntaxException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Objects;
import java.net.URI;
import org.slf4j.Logger;

public final class S3xLoginHelper
{
    private static final Logger LOG;
    public static final String LOGIN_WARNING = "The Filesystem URI contains login details. This is insecure and may be unsupported in future.";
    public static final String PLUS_WARNING = "Secret key contains a special character that should be URL encoded! Attempting to resolve...";
    public static final String PLUS_UNENCODED = "+";
    public static final String PLUS_ENCODED = "%2B";
    
    private S3xLoginHelper() {
    }
    
    public static URI buildFSURI(final URI uri) {
        Objects.requireNonNull(uri, "null uri");
        Objects.requireNonNull(uri.getScheme(), "null uri.getScheme()");
        if (uri.getHost() == null && uri.getAuthority() != null) {
            Objects.requireNonNull(uri.getHost(), "null uri host. This can be caused by unencoded / in the password string");
        }
        Objects.requireNonNull(uri.getHost(), "null uri host.");
        return URI.create(uri.getScheme() + "://" + uri.getHost());
    }
    
    public static String toString(final URI pathUri) {
        return (pathUri != null) ? String.format("%s://%s/%s", pathUri.getScheme(), pathUri.getHost(), pathUri.getPath()) : "(null URI)";
    }
    
    public static Login extractLoginDetailsWithWarnings(final URI name) {
        final Login login = extractLoginDetails(name);
        if (login.hasLogin()) {
            S3xLoginHelper.LOG.warn("The Filesystem URI contains login details. This is insecure and may be unsupported in future.");
        }
        return login;
    }
    
    public static Login extractLoginDetails(final URI name) {
        if (name == null) {
            return Login.EMPTY;
        }
        try {
            final String authority = name.getAuthority();
            if (authority == null) {
                return Login.EMPTY;
            }
            final int loginIndex = authority.indexOf(64);
            if (loginIndex < 0) {
                return Login.EMPTY;
            }
            final String login = authority.substring(0, loginIndex);
            final int loginSplit = login.indexOf(58);
            if (loginSplit > 0) {
                final String user = login.substring(0, loginSplit);
                String encodedPassword = login.substring(loginSplit + 1);
                if (encodedPassword.contains("+")) {
                    S3xLoginHelper.LOG.warn("Secret key contains a special character that should be URL encoded! Attempting to resolve...");
                    encodedPassword = encodedPassword.replaceAll("\\+", "%2B");
                }
                final String password = URLDecoder.decode(encodedPassword, "UTF-8");
                return new Login(user, password);
            }
            if (loginSplit == 0) {
                return Login.EMPTY;
            }
            return new Login(login, "");
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static URI canonicalizeUri(URI uri, final int defaultPort) {
        if (uri.getPort() == -1 && defaultPort > 0) {
            try {
                uri = new URI(uri.getScheme(), null, uri.getHost(), defaultPort, uri.getPath(), uri.getQuery(), uri.getFragment());
            }
            catch (URISyntaxException e) {
                throw new AssertionError((Object)("Valid URI became unparseable: " + uri));
            }
        }
        return uri;
    }
    
    public static void checkPath(final Configuration conf, final URI fsUri, final Path path, final int defaultPort) {
        URI pathUri = path.toUri();
        final String thatScheme = pathUri.getScheme();
        if (thatScheme == null) {
            return;
        }
        final URI thisUri = canonicalizeUri(fsUri, defaultPort);
        final String thisScheme = thisUri.getScheme();
        if (StringUtils.equalsIgnoreCase(thisScheme, thatScheme)) {
            final String thisHost = thisUri.getHost();
            String thatHost = pathUri.getHost();
            if (thatHost == null && thisHost != null) {
                final URI defaultUri = FileSystem.getDefaultUri(conf);
                if (StringUtils.equalsIgnoreCase(thisScheme, defaultUri.getScheme())) {
                    pathUri = defaultUri;
                }
                else {
                    pathUri = null;
                }
            }
            if (pathUri != null) {
                pathUri = canonicalizeUri(pathUri, defaultPort);
                thatHost = pathUri.getHost();
                if (thisHost == thatHost || (thisHost != null && StringUtils.equalsIgnoreCase(thisHost, thatHost))) {
                    return;
                }
            }
        }
        throw new IllegalArgumentException("Wrong FS " + toString(pathUri) + " -expected " + fsUri);
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3xLoginHelper.class);
    }
    
    public static class Login
    {
        private final String user;
        private final String password;
        public static final Login EMPTY;
        
        public Login() {
            this("", "");
        }
        
        public Login(final String user, final String password) {
            this.user = user;
            this.password = password;
        }
        
        public boolean hasLogin() {
            return StringUtils.isNotEmpty(this.user);
        }
        
        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            final Login that = (Login)o;
            return Objects.equals(this.user, that.user) && Objects.equals(this.password, that.password);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(this.user, this.password);
        }
        
        public String getUser() {
            return this.user;
        }
        
        public String getPassword() {
            return this.password;
        }
        
        static {
            EMPTY = new Login();
        }
    }
}
