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


import org.junit.Test;

import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class S3FileAttributesTest {

    @Test
    public void toStringPrintsBasicInfo(){
        final String key = "a key";
        final FileTime fileTime = FileTime.from(100, TimeUnit.SECONDS);
        final int size = 10;
        final boolean isDirectory = true;
        final boolean isRegularFile = true;
        S3FileAttributes fileAttributes = new S3FileAttributes(key, fileTime, size, isDirectory, isRegularFile);

        String print = fileAttributes.toString();

        assertTrue(print.contains(isRegularFile + ""));
        assertTrue(print.contains(isDirectory + ""));
        assertTrue(print.contains(size + ""));
        assertTrue(print.contains(fileTime.toString()));
        assertTrue(print.contains(key));
    }

    @Test
    public void anotherToStringPrintsBasicInfo(){
        final String key = "another complex key";
        final FileTime fileTime = FileTime.from(472931, TimeUnit.SECONDS);
        final int size = 138713;
        final boolean isDirectory = false;
        final boolean isRegularFile = false;
        S3FileAttributes fileAttributes = new S3FileAttributes(key, fileTime, size, isDirectory, isRegularFile);

        String print = fileAttributes.toString();

        assertTrue(print.contains(isRegularFile + ""));
        assertTrue(print.contains(isDirectory + ""));
        assertTrue(print.contains(size + ""));
        assertTrue(print.contains(fileTime.toString()));
        assertTrue(print.contains(key));
    }
}
