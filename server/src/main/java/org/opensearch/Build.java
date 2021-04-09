/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch;

import org.opensearch.common.Booleans;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.util.Objects;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Information about a build of OpenSearch.
 */
public class Build {
    /**
     * The current build of OpenSearch. Filled with information scanned at
     * startup from the jar.
     */
    public static final Build CURRENT;

    public enum Type {

        DEB("deb"),
        DOCKER("docker"),
        RPM("rpm"),
        TAR("tar"),
        ZIP("zip"),
        UNKNOWN("unknown");

        final String displayName;

        public String displayName() {
            return displayName;
        }

        Type(final String displayName) {
            this.displayName = displayName;
        }

        public static Type fromDisplayName(final String displayName, final boolean strict) {
            switch (displayName) {
                case "deb":
                    return Type.DEB;
                case "docker":
                    return Type.DOCKER;
                case "rpm":
                    return Type.RPM;
                case "tar":
                    return Type.TAR;
                case "zip":
                    return Type.ZIP;
                case "unknown":
                    return Type.UNKNOWN;
                default:
                    if (strict) {
                        throw new IllegalStateException("unexpected distribution type [" + displayName + "]; your distribution is broken");
                    } else {
                        return Type.UNKNOWN;
                    }
            }
        }

    }

    static {
        final Type type;
        final String hash;
        final String date;
        final boolean isSnapshot;
        final String version;

        // these are parsed at startup, and we require that we are able to recognize the values passed in by the startup scripts
        type = Type.fromDisplayName(System.getProperty("opensearch.distribution.type", "unknown"), true);

        final String opensearchPrefix = "opensearch-" + Version.CURRENT;
        final URL url = getOpenSearchCodeSourceLocation();
        final String urlStr = url == null ? "" : url.toString();
        if (urlStr.startsWith("file:/") && (
            urlStr.endsWith(opensearchPrefix + ".jar") ||
            urlStr.matches("(.*)" + opensearchPrefix + "(-)?((alpha|beta|rc)[0-9]+)?(-SNAPSHOT)?.jar")
        )) {
            try (JarInputStream jar = new JarInputStream(FileSystemUtils.openFileURLStream(url))) {
                Manifest manifest = jar.getManifest();
                hash = manifest.getMainAttributes().getValue("Change");
                date = manifest.getMainAttributes().getValue("Build-Date");
                isSnapshot = "true".equals(manifest.getMainAttributes().getValue("X-Compile-OpenSearch-Snapshot"));
                version = manifest.getMainAttributes().getValue("X-Compile-OpenSearch-Version");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // not running from the official opensearch jar file (unit tests, IDE, uber client jar, shadiness)
            hash = "unknown";
            date = "unknown";
            version = Version.CURRENT.toString();
            final String buildSnapshot = System.getProperty("build.snapshot");
            if (buildSnapshot != null) {
                try {
                    Class.forName("com.carrotsearch.randomizedtesting.RandomizedContext");
                } catch (final ClassNotFoundException e) {
                    // we are not in tests but build.snapshot is set, bail hard
                    throw new IllegalStateException("build.snapshot set to [" + buildSnapshot + "] but not running tests");
                }
                isSnapshot = Booleans.parseBoolean(buildSnapshot);
            } else {
                isSnapshot = true;
            }
        }
        if (hash == null) {
            throw new IllegalStateException("Error finding the build hash. " +
                    "Stopping OpenSearch now so it doesn't run in subtly broken ways. This is likely a build bug.");
        }
        if (date == null) {
            throw new IllegalStateException("Error finding the build date. " +
                    "Stopping OpenSearch now so it doesn't run in subtly broken ways. This is likely a build bug.");
        }
        if (version == null) {
            throw new IllegalStateException("Error finding the build version. " +
                "Stopping OpenSearch now so it doesn't run in subtly broken ways. This is likely a build bug.");
        }

        CURRENT = new Build(type, hash, date, isSnapshot, version);
    }

    private final boolean isSnapshot;

    /**
     * The location of the code source for OpenSearch
     *
     * @return the location of the code source for OpenSearch which may be null
     */
    static URL getOpenSearchCodeSourceLocation() {
        final CodeSource codeSource = Build.class.getProtectionDomain().getCodeSource();
        return codeSource == null ? null : codeSource.getLocation();
    }

    private final Type type;
    private final String hash;
    private final String date;
    private final String version;

    public Build(
        final Type type, final String hash, final String date, boolean isSnapshot,
        String version
    ) {
        this.type = type;
        this.hash = hash;
        this.date = date;
        this.isSnapshot = isSnapshot;
        this.version = version;
    }

    public String hash() {
        return hash;
    }

    public String date() {
        return date;
    }

    public static Build readBuild(StreamInput in) throws IOException {
        final String flavor;
        final Type type;
        // The following block is kept for existing BWS tests to pass.
        // TODO - clean up this code when we remove all v6 bwc tests.
        // TODO - clean this up when OSS flavor is removed in all of the code base
        //        (Integ test zip still write OSS as distribution)
        // See issue: https://github.com/opendistro-for-elasticsearch/search/issues/159
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            flavor = in.readString();
        }
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            // be lenient when reading on the wire, the enumeration values from other versions might be different than what we know
            type = Type.fromDisplayName(in.readString(), false);
        } else {
            type = Type.UNKNOWN;
        }
        String hash = in.readString();
        String date = in.readString();
        boolean snapshot = in.readBoolean();

        final String version;
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            version = in.readString();
        } else {
            version = in.getVersion().toString();
        }
        return new Build(type, hash, date, snapshot, version);
    }

    public static void writeBuild(Build build, StreamOutput out) throws IOException {
        // The following block is kept for existing BWS tests to pass.
        // TODO - clean up this code when we remove all v6 bwc tests.
        // TODO - clean this up when OSS flavor is removed in all of the code base
        // See issue: https://github.com/opendistro-for-elasticsearch/search/issues/159
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeString("oss");
        }
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            final Type buildType;
            if (out.getVersion().before(Version.V_6_7_0) && build.type() == Type.DOCKER) {
                buildType = Type.TAR;
            } else {
                buildType = build.type();
            }
            out.writeString(buildType.displayName());
        }
        out.writeString(build.hash());
        out.writeString(build.date());
        out.writeBoolean(build.isSnapshot());
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeString(build.getQualifiedVersion());
        }
    }

    /**
     * Get the version as considered at build time
     *
     * Offers a way to get the fully qualified version as configured by the build.
     * This will be the same as {@link Version} for production releases, but may include on of the qualifier ( e.x alpha1 )
     * or -SNAPSHOT for others.
     *
     * @return the fully qualified build
     */
    public String getQualifiedVersion() {
        return version;
    }

    public Type type() {
        return type;
    }

    public boolean isSnapshot() {
        return isSnapshot;
    }

    /**
     * Provides information about the intent of the build
     *
     * @return true if the build is intended for production use
     */
    public boolean isProductionRelease() {
        return version.matches("[0-9]+\\.[0-9]+\\.[0-9]+");
    }

    @Override
    public String toString() {
        return "[" + type.displayName + "][" + hash + "][" + date + "][" + version +"]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Build build = (Build) o;

        if (!type.equals(build.type)) {
            return false;
        }

        if (isSnapshot != build.isSnapshot) {
            return false;
        }
        if (hash.equals(build.hash) == false) {
            return false;
        }
        if (version.equals(build.version) == false) {
            return false;
        }
        return date.equals(build.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, isSnapshot, hash, date, version);
    }

}
