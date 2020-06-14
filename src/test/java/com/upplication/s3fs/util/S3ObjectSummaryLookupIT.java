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

/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Javier Arnáiz @arnaix
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.upplication.s3fs.util;


import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3Path;
import static org.junit.Assert.assertEquals;

public class S3ObjectSummaryLookupIT {

    private static final URI uri = URI.create("s3:///");
    private static final String bucket = EnvironmentBuilder.getBucket();

    private FileSystem fileSystemAmazon;
    private S3ObjectSummaryLookup s3ObjectSummaryLookup;

    @Before
    public void setup() throws IOException{
        fileSystemAmazon = build();
        s3ObjectSummaryLookup = new S3ObjectSummaryLookup();
    }

    private static FileSystem build() throws IOException{
        try {
            FileSystems.getFileSystem(uri).close();
            return createNewFileSystem();
        } catch(FileSystemNotFoundException e){
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem() throws IOException {
        return FileSystems.newFileSystem(uri, EnvironmentBuilder.getRealEnv());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_file() throws IOException {

        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")){

            Path base = Files.createDirectory(linux.getPath("/base"));
            Files.createFile(base.resolve("file"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        S3Path s3Path = (S3Path) path.resolve("file");
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(s3Path.getKey(), result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_file_and_exists_other_starts_with_same_name() throws IOException {
        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")){

            Path base = Files.createDirectory(linux.getPath("/base"));
            Files.createFile(base.resolve("file"));
            Files.createFile(base.resolve("file1"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        S3Path s3Path = (S3Path) path.resolve("file");
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(s3Path.getKey(), result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_a_directory() throws IOException {
        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")){

            Path base = Files.createDirectories(linux.getPath("/base").resolve("dir"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base.getParent(), new CopyDirVisitor(base.getParent(), path));
        }

        S3Path s3Path = (S3Path) path.resolve("dir");

        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(s3Path.getKey() + "/", result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_a_directory_and_exists_other_directory_starts_same_name() throws IOException {

        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        S3FileSystem s3FileSystem = (S3FileSystem)fileSystemAmazon;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0L);
        s3FileSystem.getClient().putObject(bucket.replace("/",""), startPath + "lib/angular/", new ByteArrayInputStream("".getBytes()), metadata);
        s3FileSystem.getClient().putObject(bucket.replace("/",""), startPath + "lib/angular-dynamic-locale/", new ByteArrayInputStream("".getBytes()), metadata);


        S3Path s3Path = (S3Path) s3FileSystem.getPath(bucket, startPath, "lib", "angular");
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(startPath + "lib/angular/", result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_a_directory_and_is_virtual() throws IOException {

        String folder = "angular" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        S3FileSystem s3FileSystem = (S3FileSystem)fileSystemAmazon;
        s3FileSystem.getClient().putObject(bucket.replace("/",""), key, new ByteArrayInputStream("contenido1".getBytes()), new ObjectMetadata());

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(bucket, folder);
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(key, result.getKey());
    }
}
