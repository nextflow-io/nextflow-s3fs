package com.upplication.s3fs.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import com.upplication.s3fs.AmazonS3Client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.upplication.s3fs.S3Path;

public class AmazonS3ClientMock extends AmazonS3Client {

	// TODO: map with key: bucket and value: DTO with S3Object and aclpermission

	// List<S3Object> objects = new ArrayList<S3Object>();
	// List<Bucket> buckets = new ArrayList<Bucket>();
	Map<Bucket, HashSet<S3Element>> objects = new HashMap<>();
	// default owner
	Owner owner = new Owner() {
		private static final long serialVersionUID = 5510838843790352879L;
		{
			setDisplayName("Mock");
			setId("1");
		}
	};

	public AmazonS3ClientMock() {
		super(null);
	}

	public AmazonS3ClientMock(Path base) throws IOException {
		super(null);
		// construimos el bucket
		// 1º level: buckets
		try (DirectoryStream<Path> dir = Files.newDirectoryStream(base)) {
			for (final Path bucketPath : dir) {
				BasicFileAttributes attr = Files.readAttributes(bucketPath,
						BasicFileAttributes.class);
				final Bucket bucket = new Bucket();
				bucket.setCreationDate(new Date(attr.creationTime().toMillis()));
				bucket.setName(bucketPath.getFileName().toString());
				bucket.setOwner(owner);
				final HashSet<S3Element> elemnts = new HashSet<>();
				// all s3object
				Files.walkFileTree(bucketPath, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult preVisitDirectory(Path dir,
							BasicFileAttributes attrs) throws IOException {
						if (Files.newDirectoryStream(dir).iterator().hasNext()) {
							// add only last elements
							return FileVisitResult.CONTINUE;
						} else {
							S3Element obj = parse(dir, bucketPath);

							elemnts.add(obj);
						}

						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult visitFile(Path file,
							BasicFileAttributes attrs) throws IOException {
						S3Element obj = parse(file, bucketPath);
						elemnts.add(obj);
						return FileVisitResult.CONTINUE;
					}
				});
				objects.put(bucket, elemnts);
			}
		}
	}

    /**
     * list all objects without and return ObjectListing with all elements
     * and with truncated to false
     */
	@Override
	public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
			throws AmazonClientException {
		ObjectListing objectListing = new ObjectListing();
		Integer capacity = listObjectsRequest.getMaxKeys();
		if (capacity == null) {
			capacity = Integer.MAX_VALUE;
		}

		Bucket bucket = find(listObjectsRequest.getBucketName());
		for (S3Element elem : objects.get(bucket)) {
			if (capacity > 0) {
				// TODO. add delimiter and marker support
				if (listObjectsRequest.getPrefix() != null
						&& elem.getS3Object().getKey()
								.startsWith(listObjectsRequest.getPrefix())) {
					S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
					s3ObjectSummary.setBucketName(elem.getS3Object()
							.getBucketName());
					s3ObjectSummary.setKey(elem.getS3Object().getKey());
					s3ObjectSummary.setLastModified(elem.getS3Object()
							.getObjectMetadata().getLastModified());
					s3ObjectSummary.setOwner(owner);
					s3ObjectSummary.setETag(elem.getS3Object()
							.getObjectMetadata().getETag());
					s3ObjectSummary.setSize(elem.getS3Object()
							.getObjectMetadata().getContentLength());
					objectListing.getObjectSummaries().add(s3ObjectSummary);
                    objectListing.setTruncated(false);
					capacity--;
				}
			}

		}

		return objectListing;
	}

    @Override
    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) {
        throw new UnsupportedOperationException("Not needed listObjects always return all elements");
    }

	@Override
	public Owner getS3AccountOwner() throws AmazonClientException {
		return owner;
	}

	@Override
	public List<Bucket> listBuckets() throws AmazonClientException {
		return new ArrayList<>(objects.keySet());
	}

	@Override
	public AccessControlList getObjectAcl(String bucketName, String key)
			throws AmazonClientException {

		S3Element elem = find(bucketName, key);
		if (elem != null) {
			return elem.getPermission();
		} else {
			throw new AmazonServiceException("key not found, " + key);
		}
	}

	@Override
	public AccessControlList getBucketAcl(String bucketName)
			throws AmazonClientException {

		Bucket bucket = find(bucketName);

		if (bucket == null) {
			throw new AmazonServiceException("bucket not found, " + bucketName);
		}

		AccessControlList res = createAllPermission();
		return res;
	}

	@Override
	public S3Object getObject(String bucketName, String key)
			throws AmazonClientException {
		return find(bucketName, key).getS3Object();
	}

	@Override
	public PutObjectResult putObject(String bucketName, String key, File file)
			throws AmazonClientException {

		try {
			ByteArrayInputStream stream = new ByteArrayInputStream(Files.readAllBytes(file.toPath()));
			S3Element elem = parse(stream, bucketName, key);

            persist(bucketName, elem);

			PutObjectResult putObjectResult = new PutObjectResult();
			putObjectResult.setETag("3a5c8b1ad448bca04584ecb55b836264");
			return putObjectResult;
		} catch (IOException e) {
			throw new AmazonServiceException("",e);
		}

	}
	
	public PutObjectResult putObject(String bucket, String keyName,
			InputStream inputStream, ObjectMetadata metadata) {
		S3Element elem = parse(inputStream, bucket, keyName);

        persist(bucket, elem);

		PutObjectResult putObjectResult = new PutObjectResult();
		putObjectResult.setETag("3a5c8b1ad448bca04584ecb55b836264");
		return putObjectResult;
		
	}

    /**
     * store in the memory map
     * @param bucket bucket where persist
     * @param elem
     */
    private void persist(String bucket, S3Element elem) {
        Set<S3Element> list = objects.get(find(bucket));
        // replace existing
        if (list.contains(elem)){
            list.remove(elem);
        }
        list.add(elem);
    }

    @Override
	public CopyObjectResult copyObject(String sourceBucketName,
			String sourceKey, String destinationBucketName,
			String destinationKey) throws AmazonClientException {

		S3Element element = find(sourceBucketName, sourceKey);

		if (element != null) {

			S3Object objectSource = element.getS3Object();
			// copy object with
			S3Object resObj = new S3Object();
			resObj.setBucketName(destinationBucketName);
			resObj.setKey(destinationKey);
			resObj.setObjectContent(objectSource.getObjectContent());
			resObj.setObjectMetadata(objectSource.getObjectMetadata());
			resObj.setRedirectLocation(objectSource.getRedirectLocation());
			// copy perission
			AccessControlList permission = new AccessControlList();
			permission.setOwner(element.getPermission().getOwner());
			permission.grantAllPermissions(element.getPermission().getGrants()
					.toArray(new Grant[0]));
			// maybe not exists key TODO
			objects.get(find(destinationBucketName)).add(
					new S3Element(resObj, permission, sourceKey.endsWith("/")));

			return new CopyObjectResult();
		}

		throw new AmazonServiceException("object source not found");
	}

	@Override
	public void deleteObject(String bucketName, String key)
			throws AmazonClientException {
		S3Element res = find(bucketName, key);
		if (res != null) {
			objects.get(find(bucketName)).remove(res);
		}
	}
	
	private S3Element parse(InputStream stream, String bucket, String key) {
		
		S3Object object = new S3Object();
		
		object.setBucketName(bucket);
		object.setKey(key);

        byte[] content;
        try {
            content = IOUtils.toByteArray(stream);
        }
        catch (IOException e) {
            throw new IllegalStateException("the stream is closed", e);
        }

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setLastModified(new Date());
        metadata.setContentLength(content.length);

        object.setObjectContent(new ByteArrayInputStream(content));
		object.setObjectMetadata(metadata);
		// TODO: create converter between path permission and s3 permission
		AccessControlList permission = createAllPermission();
		return new S3Element(object, permission, false);
	}
	
	private S3Element parse(Path elem, Path bucket) throws IOException{
		boolean dir = false;
		if (Files.isDirectory(elem)) {
			dir = true;
		}

		S3Object object = new S3Object();
		
		object.setBucketName(bucket.getFileName().toString());
		
		String key = bucket.relativize(elem).toString();
		if (dir) {
			key += "/";
		}		
		object.setKey(key);

		ObjectMetadata metadata = new ObjectMetadata();
		BasicFileAttributes attr = Files.readAttributes(elem,
				BasicFileAttributes.class);
		metadata.setLastModified(new Date(attr.lastAccessTime().toMillis()));
		if (dir) {
			metadata.setContentLength(0);
			object.setObjectContent(null);
		} else {
			metadata.setContentLength(attr.size());
			object.setObjectContent( new ByteArrayInputStream(Files.readAllBytes(elem)));
		}

		object.setObjectMetadata(metadata);
		// TODO: create converter between path permission and s3 permission
		AccessControlList permission = createAllPermission();

		return new S3Element(object, permission, dir);
	}

	private AccessControlList createAllPermission() {
		AccessControlList res = new AccessControlList();
		res.setOwner(getS3AccountOwner());
		Grantee grant = new Grantee() {

			@Override
			public void setIdentifier(String id) {
			}

			@Override
			public String getTypeIdentifier() {
				return getS3AccountOwner().getId();
			}

			@Override
			public String getIdentifier() {
				return getS3AccountOwner().getId();
			}
		};

		res.grantPermission(grant, Permission.FullControl);
		res.grantPermission(grant, Permission.Read);
		res.grantPermission(grant, Permission.Write);
		return res;
	}

	private S3Element find(String bucketName, String key) {
		Bucket bucket = find(bucketName);
		if (bucket == null) {
			return null;
		}

		for (S3Element elemnt : objects.get(bucket)) {
			String newKey = key;
			if (elemnt.isDirectory()) {
				if (!key.endsWith("/")) {
					newKey += "/";
				}
			}
			if (elemnt.getS3Object().getKey().equals(newKey)) {
				return elemnt;
			}
		}

		return null;
	}

	private Bucket find(String bucketName) {
		for (Bucket bucket : objects.keySet()) {
			if (bucket.getName().equals(bucketName)) {
				return bucket;
			}
		}
		return null;
	}

	public static class S3Element {

		private S3Object s3Object;
        private boolean directory;
		private AccessControlList permission;

		public S3Element(S3Object s3Object, AccessControlList permission,
				boolean directory) {
			this.s3Object = s3Object;
			this.directory = directory;
			this.permission = permission;
		}

		public S3Object getS3Object() {
			return s3Object;
		}

		public void setS3Object(S3Object s3Object) {
			this.s3Object = s3Object;
		}

		public AccessControlList getPermission() {
			return permission;
		}

		public boolean isDirectory() {
			return directory;
		}

		public void setDirectory(boolean directory) {
			this.directory = directory;
		}

		public void setPermission(AccessControlList permission) {
			this.permission = permission;
		}


        @Override
        public boolean equals(Object object){

            if (object == null){
                return false;
            }

            if (object instanceof S3Element){
                S3Element elem = (S3Element)object;
                // only is the same if bucketname and key are not null and are the same
                if (elem.getS3Object() != null && this.getS3Object() != null &&
                        elem.getS3Object().getBucketName() != null &&
                        elem.getS3Object().getBucketName().equals(this.getS3Object().getBucketName()) &&
                        elem.getS3Object().getKey() != null && elem.getS3Object().getKey().equals(this.getS3Object().getKey())){
                    return true;
                }

                return false;
            }
            else{
                return false;
            }

        }

        @Override
        public int hashCode() {
            int result = s3Object != null && s3Object.getBucketName() != null ? s3Object.getBucketName().hashCode() : 0;
            result = 31 * result + (s3Object != null && s3Object.getKey() != null? s3Object.getKey().hashCode() : 0);
            return result;
        }
    }
}