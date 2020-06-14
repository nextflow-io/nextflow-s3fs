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

package com.upplication.s3fs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ServiceLoader;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.util.FileTypeDetector;
import org.apache.tika.Tika;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class FileTypeDetectorTest {
    private FileSystem fsMem;
    @Before
    public void cleanup() throws IOException {
        fsMem = MemoryFileSystemBuilder.newLinux().build("linux");
    }

    @After
    public void closeMemory() throws IOException{
        fsMem.close();
    }

    @Test
    public void fileTypeDetectorUseTike() throws IOException {

        Tika tika = spy(new Tika());
        FileTypeDetector detector = new FileTypeDetector(tika);
        Path path = fsMem.getPath("/file.html");
        Files.write(path, "<html><body>ey</body></html>".getBytes());
        detector.probeContentType(path);

        verify(tika).detect(any(InputStream.class), eq(path.getFileName().toString()));
    }

    @Test
    public void fileTypeDetectorDetectByServiceLocator() throws IOException {
        // act
        ServiceLoader<java.nio.file.spi.FileTypeDetector> loader = ServiceLoader
                .load(java.nio.file.spi.FileTypeDetector.class, ClassLoader.getSystemClassLoader());
        // assert
        boolean existsS3fsFileTypeDetector = false;
        for (java.nio.file.spi.FileTypeDetector installed : loader) {
            if (installed instanceof FileTypeDetector){
                existsS3fsFileTypeDetector = true;
            }
        }

        assertTrue(existsS3fsFileTypeDetector);
    }
}
