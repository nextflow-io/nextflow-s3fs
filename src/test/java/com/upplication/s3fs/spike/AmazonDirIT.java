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

package com.upplication.s3fs.spike;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.UUID;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import static com.upplication.s3fs.util.EnvironmentBuilder.getBucket;
import static com.upplication.s3fs.util.EnvironmentBuilder.getRealEnv;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AmazonDirIT {

    private static final URI uri = URI.create("s3:///");

	@Test
	public void createDirWithoutEndSlash() throws IOException{
		
		S3FileSystemProvider provider = new S3FileSystemProvider(){
			/**
			 * Nueva implementación: probamos si funcionaria
			 */
			@Override
			public void createDirectory(Path dir, FileAttribute<?>... attrs)
					throws IOException {
				S3Path s3Path = (S3Path) dir;
				
				Preconditions.checkArgument(attrs.length == 0,
						"attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO

				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(0);

				s3Path.getFileSystem()
						.getClient()
						.putObject(s3Path.getBucket(), s3Path.getKey(),
								new ByteArrayInputStream(new byte[0]), metadata);
			}
		};
		
		FileSystem fileSystem = provider.newFileSystem(uri, getRealEnv());
		
		String name = UUID.randomUUID().toString();
		
		Path dir = fileSystem.getPath(getBucket(), name);
		
		Files.createDirectory(dir);
		
		assertTrue(Files.exists(dir));
		
		// añadimos mas ficheros dentro:
		
		Path dir2 = fileSystem.getPath(getBucket(), name);
		
		// como se si un fichero es directorio? en amazon pueden existir 
		// tanto como directorios como ficheros con el mismo nombre
		assertTrue(!Files.isDirectory(dir2));
		
		fileSystem.close();
	}
	
	@Test
	public void testCreatedFromAmazonWebConsoleNotExistKeyForFolder() throws IOException{
		S3FileSystemProvider provider = new S3FileSystemProvider();
		
		String folder = UUID.randomUUID().toString();
		String file1 = folder+"/file.html";
		
		FileSystem fileSystem = provider.newFileSystem(uri, getRealEnv());
		Path dir = fileSystem.getPath(getBucket(), folder);
		
		S3Path s3Path = (S3Path)dir;
		// subimos un fichero sin sus paths
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		s3Path.getFileSystem().getClient().putObject(s3Path.getBucket(), file1,
				new ByteArrayInputStream(new byte[0]), metadata);
		
		// para amazon no existe el path: folder
		try{
			s3Path.getFileSystem().getClient().getObjectMetadata(s3Path.getBucket(), s3Path.getKey());
            fail("expected AmazonS3Exception");
		}
		catch(AmazonS3Exception e){
			assertEquals(404, e.getStatusCode());
		}
	}
}
