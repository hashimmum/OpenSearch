/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobPath;

import java.util.Locale;
import java.util.Set;

import static org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory.SEGMENTS;
import static org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory.LOCK_FILES;
import static org.opensearch.index.translog.RemoteFsTranslog.DATA_DIR;
import static org.opensearch.index.translog.RemoteFsTranslog.METADATA_DIR;
import static org.opensearch.index.translog.RemoteFsTranslog.TRANSLOG;

/**
 * Enumerates the types of remote store paths resolution techniques supported by OpenSearch.
 * For more information, see <a href="https://github.com/opensearch-project/OpenSearch/issues/12567">Github issue #12567</a>.
 *
 * @opensearch.internal
 */
public enum RemoteStorePathType {

    FIXED {
        @Override
        public BlobPath generatePath(BlobPath basePath, String indexUUID, String shardId, String dataCategory, String dataType) {
            return basePath.add(indexUUID).add(shardId).add(dataCategory).add(dataType);
        }
    },
    HASHED_PREFIX {
        @Override
        public BlobPath generatePath(BlobPath basePath, String indexUUID, String shardId, String dataCategory, String dataType) {
            // TODO - We need to implement this, keeping the same path as Fixed for sake of multiple tests that can fail otherwise.
            // throw new UnsupportedOperationException("Not implemented"); --> Not using this for unblocking couple of tests.
            return basePath.add(indexUUID).add(shardId).add(dataCategory).add(dataType);
        }
    };

    /**
     * @param basePath     base path of the underlying blob store repository
     * @param indexUUID    of the index
     * @param shardId      shard id
     * @param dataCategory is either translog or segment
     * @param dataType     can be one of data, metadata or lock_files.
     * @return the blob path for the underlying remote store path type.
     */
    public BlobPath path(BlobPath basePath, String indexUUID, String shardId, String dataCategory, String dataType) {
        assertDataCategoryAndTypeCombination(dataCategory, dataType);
        return generatePath(basePath, indexUUID, shardId, dataCategory, dataType);
    }

    abstract BlobPath generatePath(BlobPath basePath, String indexUUID, String shardId, String dataCategory, String dataType);

    /**
     * This method verifies that if the data category is translog, then the data type can not be lock_files. All other
     * combination of data categories and data types are possible.
     */
    private static void assertDataCategoryAndTypeCombination(String dataCategory, String dataType) {
        assert Set.of(TRANSLOG, SEGMENTS).contains(dataCategory);
        assert Set.of(DATA_DIR, METADATA_DIR, LOCK_FILES).contains(dataType);
        assert TRANSLOG.equals(dataCategory) == false || LOCK_FILES.equals(dataType) == false;
    }

    public static RemoteStorePathType parseString(String remoteStoreBlobPathType) {
        try {
            return RemoteStorePathType.valueOf(remoteStoreBlobPathType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            // IllegalArgumentException is thrown when the input does not match any enum name
            // NullPointerException is thrown when the input is null
            throw new IllegalArgumentException("Could not parse RemoteStorePathType for [" + remoteStoreBlobPathType + "]");
        }
    }

    /**
     * This string is used as key for storing information in the custom data in index settings.
     */
    public static final String NAME = "path_type";
}
