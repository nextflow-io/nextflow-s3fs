/*
 * Copyright 2020, Seqera Labs
 * Copyright 2013-2019, Centre for Genomic Regulation (CRG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.upplication.s3fs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Random;
import java.util.UUID;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.upplication.s3fs.util.EnvironmentBuilder;
import com.upplication.s3fs.util.S3UploadRequest;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


/**
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
public class S3OutputStreamTest {

    private static final URI uri = URI.create("s3:///");
    private static final String bucket = EnvironmentBuilder.getBucket();
    private static int _1MB = 1024 * 1024;
    private static int _1KB = 1024;

    private S3FileSystem s3FileSystem;

    private static S3FileSystem build() throws IOException{
        try {
            FileSystems.getFileSystem(uri).close();
            return (S3FileSystem)createNewFileSystem();
        } catch(FileSystemNotFoundException e){
            return (S3FileSystem)createNewFileSystem();
        }
    }


    private static FileSystem createNewFileSystem() throws IOException {
        return FileSystems.newFileSystem(uri, EnvironmentBuilder.getRealEnv());
    }

    @Before
    public void setup() throws IOException {
        s3FileSystem = build();
    }

    @After
    public void cleanup() {
        S3OutputStream.shutdownExecutor();
    }

    static long copy(String str, OutputStream sink) throws IOException {
        return copy(new ByteArrayInputStream(str.getBytes()), sink);
    }

    static long copy(byte[] bytes, OutputStream sink) throws IOException {
        return  copy(new ByteArrayInputStream(bytes), sink);
    }

    static long copy(InputStream source, OutputStream sink)
            throws IOException
    {
        long nread = 0L;
        byte[] buf = new byte[8192];
        int n;
        while ((n = source.read(buf)) > 0) {
            sink.write(buf, 0, n);
            nread += n;
        }
        return nread;
    }

    static byte[] randomBytes(int length) {
        byte[] result = new byte[length];
        new Random().nextBytes(result);
        return result;
    }

    @Test
    public void testEmptyFileUpload() throws IOException {
        // create a random S3 path
        S3Path path = (S3Path) s3FileSystem.getPath(bucket, UUID.randomUUID().toString());
        S3OutputStream out = new S3OutputStream(s3FileSystem.getClient().client, path.toS3ObjectId());

        final byte[] EMPTY = new byte[] {};
        copy(EMPTY, out);
        out.close();

        // read the file
        byte[] copy = Files.readAllBytes(path);

        assertEquals(0, copy.length);
    }

    @Test
    public void testSmallUpload() throws IOException {

        // random string
        String str = "Hello world!";

        // create a random S3 path
        S3Path path = (S3Path) s3FileSystem.getPath(bucket, UUID.randomUUID().toString());
        S3OutputStream out = new S3OutputStream(s3FileSystem.getClient().client, path.toS3ObjectId());

        copy(str, out);
        out.close();

        // read the file
        String copy = new String(Files.readAllBytes(path));

        assertEquals(0, out.getPartsCount());
        assertEquals(str, copy);
    }

    @Test
    public void testMediumUpload() throws IOException {

        byte[] payload = randomBytes(100 * _1KB);

        // create a random S3 path
        S3Path path = (S3Path) s3FileSystem.getPath(bucket, UUID.randomUUID().toString());
        S3OutputStream out = new S3OutputStream(s3FileSystem.getClient().client, path.toS3ObjectId());

        copy(payload, out);
        out.close();

        // read the file
        byte[] copy = Files.readAllBytes(path);

        assertEquals(0, out.getPartsCount());
        assertArrayEquals(payload, copy);
    }

    @Test
    public void testMultipartUploadTwoChunks() throws IOException {

        /**
         * Verifies that multipart multipart upload works correctly when the payload
         * is exactly a multiple of the upload chunk size
         */

        byte[] payload = randomBytes(10 * _1MB);

        // create a random S3 path
        S3Path path = (S3Path) s3FileSystem.getPath(bucket, UUID.randomUUID().toString());

        // create the upload request
        S3UploadRequest req = new S3UploadRequest()
                .setChunkSize(5 * _1MB)
                .setObjectId(path.toS3ObjectId());
        S3OutputStream out = new S3OutputStream(s3FileSystem.getClient().client, req);

        copy(payload, out);
        out.close();

        // read the file
        byte[] copy = Files.readAllBytes(path);

        assertArrayEquals(payload, copy);
        assertEquals(2, out.getPartsCount());
    }

    @Test
    public void testEncryptedObjects() throws IOException {
    
        byte[] payload = randomBytes(2 * _1MB);

        String objectKey = UUID.randomUUID().toString();

        S3Path path = (S3Path) s3FileSystem.getPath(bucket, objectKey);
        S3UploadRequest req = new S3UploadRequest()
                                .setObjectId(path.toS3ObjectId())
                                .setStorageEncryption("AES256");
        S3OutputStream out = new S3OutputStream(s3FileSystem.getClient().client, req);
        copy(payload, out);
        out.close();
        
        ObjectMetadata sourceObjMetadata = s3FileSystem.getClient().getObjectMetadata(bucket, objectKey);
        assertEquals("AES256",sourceObjMetadata.getSSEAlgorithm()); 
        
        S3Path copyPath = (S3Path) s3FileSystem.getPath(bucket, objectKey+"_copy");
        S3FileSystemProvider provider = new S3FileSystemProvider();
        provider.copy(path,copyPath);
        
        ObjectMetadata copyObjMetadata = s3FileSystem.getClient().getObjectMetadata(bucket, objectKey+"_copy");
        assertEquals("AES256",copyObjMetadata.getSSEAlgorithm()); 
    }


    @Test
    public void testMultipartUploadMoreChunks() throws IOException {

        /*
         * since it uploads 17 MG and upload chunk size is 5 MB ==> 4 parts
         */
        byte[] payload = randomBytes(17 * _1MB);
        
        // create a random S3 path
        S3Path path = (S3Path) s3FileSystem.getPath(bucket, UUID.randomUUID().toString());

        // create the upload request
        S3UploadRequest req = new S3UploadRequest()
                                .setChunkSize(5 * _1MB)
                                .setObjectId(path.toS3ObjectId());
        S3OutputStream out = new S3OutputStream(s3FileSystem.getClient().client, req);

        copy(payload, out);
        out.close();

        // read the file
        byte[] copy = Files.readAllBytes(path);

        assertArrayEquals(payload, copy);
        assertEquals(4, out.getPartsCount());
    }

    @Test
    public void testMultipartUploadManyFlushes() throws IOException {

        /*
         * Verifies that multipart multipart upload works correctly when the payload
         * is exactly a multiple of the upload chunk size
         */

        byte[] payload = randomBytes(6 * _1MB);

        // create a random S3 path
        S3Path path = (S3Path) s3FileSystem.getPath(bucket, UUID.randomUUID().toString());

        // create the upload request
        S3UploadRequest req = new S3UploadRequest()
                                    .setChunkSize(5 * _1MB)
                                    .setObjectId(path.toS3ObjectId());
        S3OutputStream out = new S3OutputStream(s3FileSystem.getClient().client, req);
        out.flush();
        copy(payload, out);
        out.flush();
        out.flush();
        out.close();

        // read the file
        byte[] copy = Files.readAllBytes(path);

        assertArrayEquals(payload, copy);
        assertEquals(2, out.getPartsCount());
    }


}
