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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.FilesOperationsIT;
import static com.upplication.s3fs.S3FileSystemProvider.ACCESS_KEY;
import static com.upplication.s3fs.S3FileSystemProvider.SECRET_KEY;
/**
 * Test Helper
 */
public abstract class EnvironmentBuilder {
	
	public static final String BUCKET_NAME_KEY = "bucket_name";
	/**
	 * Get credentials from environment vars, and if not found from amazon-test.properties
	 * @return Map with the credentials
	 */
	public static Map<String, Object> getRealEnv(){
		Map<String, Object> env = null;
		
		String accessKey = System.getenv(ACCESS_KEY);
		String secretKey = System.getenv(SECRET_KEY);
		
		if (accessKey != null && secretKey != null){
			env = ImmutableMap.<String, Object> builder()
				.put(ACCESS_KEY, accessKey)
				.put(SECRET_KEY, secretKey).build();
		}
		else{
			final Properties props = new Properties();
			try {
				props.load(EnvironmentBuilder.class.getResourceAsStream("/amazon-test.properties"));
			} catch (IOException e) {
				throw new RuntimeException("not found amazon-test.properties in the classpath", e);
			}
			env = ImmutableMap.<String, Object> builder()
					.put(ACCESS_KEY, props.getProperty(ACCESS_KEY))
					.put(SECRET_KEY, props.getProperty(SECRET_KEY)).build();
		}
		
		return env;
	}
	/**
	 * get default bucket name
	 * @return String without end separator
	 */
	public static String getBucket(){
		
		String bucketName = System.getenv(BUCKET_NAME_KEY);
		if (bucketName != null){
			return bucketName;
		}
		else{
			final Properties props = new Properties();
			try {
				props.load(FilesOperationsIT.class.getResourceAsStream("/amazon-test.properties"));
				return props.getProperty(BUCKET_NAME_KEY);
			} catch (IOException e) {
				throw new RuntimeException("needed /amazon-test.properties in the classpath");
			}
		}
	}
}
