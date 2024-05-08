/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TranslogCkpFilesTransferTrackerTests extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);
    protected long primaryTerm = 10;
    protected long generation = 5;
    protected long minTranslogGeneration = 2;
    FileTransferTracker fileTransferTracker;
    RemoteTranslogTransferTracker remoteTranslogTransferTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        fileTransferTracker = FileTransferTrackerFactory.getFileTransferTracker(shardId, remoteTranslogTransferTracker, false);
    }

    public void testOnSuccess() throws IOException {
        Path testFile = createTempFile();
        Path ckpFile = createTempFile();
        int fileSize = 128;
        int ckpFileSize = 100;
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        Files.write(ckpFile, randomByteArrayOfLength(ckpFileSize), StandardOpenOption.APPEND);
        TranslogCheckpointSnapshot transferFileSnapshot = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            testFile,
            ckpFile,
            null,
            null,
            null,
            generation
        );
        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(transferFileSnapshot);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        // idempotent
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        assertEquals(fileTransferTracker.allUploadedGeneration().size(), 1);
        assertEquals(fileTransferTracker.allUploadedFiles().size(), 2);
        try {
            remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
            fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
            fail("failure after succcess invalid");
        } catch (IllegalStateException ex) {
            // all good
        }
    }

    public void testOnFailure() throws IOException {
        Path tlogFile1 = createTempFile();
        Path ckpFile1 = createTempFile();
        Path tlogFile2 = createTempFile();
        Path ckpFile2 = createTempFile();
        int fileSize = 128;
        Files.write(tlogFile1, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        Files.write(ckpFile1, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);

        Files.write(tlogFile2, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        Files.write(ckpFile2, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        TranslogCheckpointSnapshot translogCheckpointSnapshot1 = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            tlogFile1,
            ckpFile1,
            null,
            null,
            null,
            generation
        );
        TranslogCheckpointSnapshot translogCheckpointSnapshot2 = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation + 1,
            minTranslogGeneration,
            tlogFile2,
            ckpFile2,
            null,
            null,
            null,
            generation + 1
        );
        TransferFileSnapshot checkpointFileSnapshot1 = translogCheckpointSnapshot1.getCheckpointFileSnapshot();
        TransferFileSnapshot translogFileSnapshot1 = translogCheckpointSnapshot1.getTranslogFileSnapshot();
        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(translogCheckpointSnapshot1);
        toUpload.add(translogCheckpointSnapshot2);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize * 4);
        fileTransferTracker.onFailure(
            translogCheckpointSnapshot1,
            new TranslogTransferException(
                translogCheckpointSnapshot1,
                new IOException("random exception"),
                Set.of(checkpointFileSnapshot1, translogFileSnapshot1),
                null
            )
        );
        fileTransferTracker.onSuccess(translogCheckpointSnapshot2);
        assertEquals(fileTransferTracker.allUploadedGeneration().size(), 1);
        assertEquals(fileTransferTracker.allUploadedFiles().size(), 2);

        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize * 2);
        fileTransferTracker.onSuccess(translogCheckpointSnapshot1);
        assertEquals(fileTransferTracker.allUploadedGeneration().size(), 2);
        assertEquals(fileTransferTracker.allUploadedFiles().size(), 4);

        checkpointFileSnapshot1.close();
        translogFileSnapshot1.close();
    }

    public void testOnSuccessStatsFailure() throws IOException {
        RemoteTranslogTransferTracker localRemoteTranslogTransferTracker = spy(remoteTranslogTransferTracker);
        doAnswer((count) -> { throw new NullPointerException("Error while updating stats"); }).when(localRemoteTranslogTransferTracker)
            .addUploadBytesSucceeded(anyLong());

        FileTransferTracker localFileTransferTracker = FileTransferTrackerFactory.getFileTransferTracker(
            shardId,
            localRemoteTranslogTransferTracker,
            false
        );

        Path testFile = createTempFile();
        int fileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);

        TranslogCheckpointSnapshot transferFileSnapshot = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            testFile,
            testFile,
            null,
            null,
            null,
            generation
        );

        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(transferFileSnapshot);
        localFileTransferTracker.recordBytesForFiles(toUpload);
        localRemoteTranslogTransferTracker.addUploadBytesStarted(2 * fileSize);
        localFileTransferTracker.onSuccess(transferFileSnapshot);
        assertEquals(localFileTransferTracker.allUploadedGeneration().size(), 1);
    }

    public void testUploaded() throws IOException {
        Path testFile = createTempFile();
        Path ckpFile = createTempFile();
        int fileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        TranslogCheckpointSnapshot transferFileSnapshot = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            testFile,
            ckpFile,
            null,
            null,
            null,
            generation
        );

        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(transferFileSnapshot);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(2 * fileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        String tlogFileName = String.valueOf(testFile.getFileName());
        String ckpFileName = String.valueOf(ckpFile.getFileName());
        assertTrue(fileTransferTracker.isFileUploaded(tlogFileName));
        assertTrue(fileTransferTracker.isFileUploaded(ckpFileName));
        assertTrue(fileTransferTracker.isGenerationUploaded(generation));
        assertFalse(fileTransferTracker.isGenerationUploaded(generation + 2));
        assertFalse(fileTransferTracker.isFileUploaded("random-name"));

        fileTransferTracker.deleteGenerations(Set.of(generation));
        assertFalse(fileTransferTracker.isGenerationUploaded(generation));

        fileTransferTracker.deleteFiles(List.of(tlogFileName));
        assertFalse(fileTransferTracker.isFileUploaded(tlogFileName));
        assertTrue(fileTransferTracker.isFileUploaded(ckpFileName));

    }

}
