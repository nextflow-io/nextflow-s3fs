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


import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmazonS3ClientTest {

    private AmazonS3 amazonS3;
    private AmazonS3Client amazonS3Client;

    @Before
    public void setup(){
        amazonS3 = mock(AmazonS3.class);
        amazonS3Client = spy(new AmazonS3Client(amazonS3));
    }

    @Test
    public void putObjectInputStream(){

        String bucket = "bucket";
        String keyName = "keyName";
        InputStream inputStream = mock(InputStream.class);
        ObjectMetadata metadata = new ObjectMetadata();
        PutObjectResult expectedResult = mock(PutObjectResult.class);
        when(amazonS3.putObject(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class))).thenReturn(expectedResult);

        PutObjectResult actualResult = amazonS3Client.putObject(bucket, keyName, inputStream, metadata);

        verify(amazonS3).putObject(eq(bucket), eq(keyName), eq(inputStream), eq(metadata));
        assertEquals(expectedResult, actualResult);
    }


    @Test
    public void putObjectFile(){

        String bucket = "bucket";
        String keyName = "keyName";
        File file = mock(File.class);
        PutObjectResult expectedResult = mock(PutObjectResult.class);
        when(amazonS3.putObject(anyString(), anyString(), any(File.class))).thenReturn(expectedResult);

        PutObjectResult actualResult =  amazonS3Client.putObject(bucket, keyName, file);

        verify(amazonS3).putObject(eq(bucket), eq(keyName), eq(file));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void deleteObjerct(){

        String bucket = "bucket";
        String keyName = "keyName";

        amazonS3Client.deleteObject(bucket, keyName);

        verify(amazonS3).deleteObject(eq(bucket), eq(keyName));
    }

    @Test
    public void getObject(){

        String bucket = "bucket";
        String keyName = "keyName";
        S3Object expectedResult = mock(S3Object.class);
        when(amazonS3.getObject(anyString(), anyString())).thenReturn(expectedResult);

        S3Object actualResult = amazonS3Client.getObject(bucket, keyName);

        verify(amazonS3).getObject(eq(bucket), eq(keyName));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void copyObject(){

        String bucketSource = "bucket";
        String keyNameSource = "keyName";

        String bucketDest = "bucketDest";
        String keyNameDest = "keyNameDest";

        CopyObjectResult expectedResult = mock(CopyObjectResult.class);
        when(amazonS3.copyObject(anyString(), anyString(), anyString(), anyString())).thenReturn(expectedResult);

        CopyObjectResult actualResult = amazonS3Client.copyObject(bucketSource, keyNameSource, bucketDest, keyNameDest);

        verify(amazonS3).copyObject(eq(bucketSource), eq(keyNameSource), eq(bucketDest), eq(keyNameDest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void getBucketAcl(){

        String bucket = "bucket";
        AccessControlList expectedResult = mock(AccessControlList.class);
        when(amazonS3.getBucketAcl(anyString())).thenReturn(expectedResult);

        AccessControlList actualResult = amazonS3Client.getBucketAcl(bucket);

        verify(amazonS3).getBucketAcl(eq(bucket));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void getObjectMetadata(){

        String bucket = "bucket";
        String keyName = "keyName";
        ObjectMetadata expectedResult = mock(ObjectMetadata.class);
        when(amazonS3.getObjectMetadata(anyString(), anyString())).thenReturn(expectedResult);

        ObjectMetadata actualResult = amazonS3Client.getObjectMetadata(bucket, keyName);

        verify(amazonS3).getObjectMetadata(eq(bucket), eq(keyName));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void getObjectAcl(){

        String bucket = "bucket";
        String keyName = "keyName";
        AccessControlList expectedResult = mock(AccessControlList.class);
        when(amazonS3.getObjectAcl(anyString(), anyString())).thenReturn(expectedResult);

        AccessControlList actualResult = amazonS3Client.getObjectAcl(bucket, keyName);

        verify(amazonS3).getObjectAcl(eq(bucket), eq(keyName));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void getS3AccountOwner(){

        Owner expectedResult = mock(Owner.class);
        when(amazonS3.getS3AccountOwner()).thenReturn(expectedResult);

        Owner actualResult = amazonS3Client.getS3AccountOwner();

        verify(amazonS3).getS3AccountOwner();
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void listBuckets(){

        List<Bucket> expectedResult = mock(List.class);
        when(amazonS3.listBuckets()).thenReturn(expectedResult);

        List<Bucket> actualResult = amazonS3Client.listBuckets();

        verify(amazonS3).listBuckets();
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void listObjects(){

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        ObjectListing expectedResult = mock(ObjectListing.class);
        when(amazonS3.listObjects(any(ListObjectsRequest.class))).thenReturn(expectedResult);

        ObjectListing actualResult = amazonS3Client.listObjects(listObjectsRequest);

        verify(amazonS3).listObjects(eq(listObjectsRequest));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void listNextBatchOfObjects(){

        ObjectListing objectListing = new ObjectListing();
        ObjectListing expectedResult = mock(ObjectListing.class);
        when(amazonS3.listNextBatchOfObjects(any(ObjectListing.class))).thenReturn(expectedResult);

        ObjectListing actualResult = amazonS3Client.listNextBatchOfObjects(objectListing);

        verify(amazonS3).listNextBatchOfObjects(eq(objectListing));
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void setEndpoint(){

        String endpoint = "endpoint";

        amazonS3Client.setEndpoint(endpoint);

        verify(amazonS3).setEndpoint(eq(endpoint));
    }
}
