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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.upplication.s3fs.S3FileSystemProvider;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * FileSystems.newFileSystem busca mediante el serviceLoader los
 * posibles fileSystemsProvider y los llama con newFileSystem.
 * Si
 * @author jarnaiz
 *
 */
public class InstallProviderTest {
	
	@Before
	public void cleanup(){
		//clean resources
		try{
			FileSystems.getFileSystem(URI.create("s3:///")).close();
		}
		catch(FileSystemNotFoundException | IOException e){}
	}
	
	@Test
	public void useZipProvider() throws IOException{
		
		Path path = createZipTempFile();
		String pathFinal = pathToString(path);
		
		FileSystem fs = FileSystems.newFileSystem(URI.create("jar:file:" + pathFinal), new HashMap<String,Object>(), this.getClass().getClassLoader());
		Path zipPath = fs.getPath("test.zip");
	
		assertNotNull(zipPath);
		assertNotNull(zipPath.getFileSystem());
		assertNotNull(zipPath.getFileSystem().provider());
		//assertTrue(zipPath.getFileSystem().provider() instanceof com.sun.nio.zipfs.ZipFileSystemProvider);
	}
	
	@Test(expected = FileSystemNotFoundException.class)
	public void useZipProviderPathNotExists() throws IOException{
		FileSystems.newFileSystem(URI.create("jar:file:/not/exists/zip.zip"), new HashMap<String,Object>(), this.getClass().getClassLoader());
	}

	
	@Test
	public void useAlternativeZipProvider() throws IOException{
		
		Path path = createZipTempFile();
		String pathFinal = pathToString(path);
		
		FileSystem fs = FileSystems.newFileSystem(URI.create("zipfs:file:" + pathFinal), new HashMap<String,Object>(), this.getClass().getClassLoader());
		
		Path zipPath = fs.getPath("test.zip");
	
		assertNotNull(zipPath);
		assertNotNull(zipPath.getFileSystem());
		assertNotNull(zipPath.getFileSystem().provider());
		assertTrue(zipPath.getFileSystem().provider() instanceof com.github.marschall.com.sun.nio.zipfs.ZipFileSystemProvider);
	}
	
	@Test
	public void newS3Provider() throws IOException{
		URI uri = URI.create("s3:///hola/que/tal/");
		// if meta-inf/services/java.ni.spi.FileSystemProvider is not present with
		// the content: com.upplication.s3fs.S3FileSystemProvider
		// this method return ProviderNotFoundException
		FileSystem fs = FileSystems.newFileSystem(uri, new HashMap<String,Object>(), this.getClass().getClassLoader());

		Path path = fs.getPath("test.zip");
		assertNotNull(path);
		assertNotNull(path.getFileSystem());
		assertNotNull(path.getFileSystem().provider());
		assertTrue(path.getFileSystem().provider() instanceof S3FileSystemProvider);
		// close fs (FileSystems.getFileSystem throw exception)
		fs.close();
	}
	
	@Test(expected=FileSystemNotFoundException.class)
	public void getZipProvider() throws IOException{
		URI uri = URI.create("jar:file:/file.zip");
		FileSystems.getFileSystem(uri);
	}
	
	@Test(expected=FileSystemNotFoundException.class)
	public void getS3Provider() throws IOException{
		URI uri = URI.create("s3:///hola/que/tal/");
		FileSystems.getFileSystem(uri);
	}
	
	// desviaton from spec
	@Test(expected=FileSystemNotFoundException.class)
	public void getZipPath(){
		Paths.get(URI.create("jar:file:/file.zip!/BAR"));
	}
	// desviation from spec
	@Test(expected=FileSystemNotFoundException.class)
	public void getMemoryPath(){
		Paths.get(URI.create("memory:hellou:/file.zip"));
	}
	
	// ~ helpers methods
	
	private Path createZipTempFile() throws IOException{
		File zip = Files.createTempFile("temp", ".zip").toFile();
		
		try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zip))){
			ZipEntry entry = new ZipEntry("text.txt");
			out.putNextEntry(entry);
		}
		
		return zip.toPath();
	}

	private String pathToString(Path pathNext) {
		StringBuilder pathFinal = new StringBuilder();
		
		for (; pathNext.getParent() != null; pathNext = pathNext.getParent()){
			pathFinal.insert(0,  "/" + pathNext.getFileName().toString());
		}
		return pathFinal.toString();
	}	
}
