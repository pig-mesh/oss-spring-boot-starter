/*
 *    Copyright (c) 2018-2025, lengleng All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * Neither the name of the pig4cloud.com developer nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * Author: lengleng (wangiegie@gmail.com)
 */

package com.pig4cloud.plugin.oss.service;

import com.pig4cloud.plugin.oss.OssProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * aws-s3 通用存储操作 支持所有兼容s3协议的云存储: {阿里云OSS，腾讯云COS，七牛云，京东云，minio 等}
 *
 * @author lengleng
 * @author 858695266
 * @author L.cm
 * @date 2020/5/23 6:36 上午
 * @since 1.0
 */
@RequiredArgsConstructor
public class OssTemplate implements InitializingBean {

	private final OssProperties ossProperties;

	private S3Client s3Client;

	private S3Presigner s3Presigner;

	/**
	 * 创建bucket
	 * @param bucketName bucket名称
	 */
	public void createBucket(String bucketName) {
		if (!headBucket(bucketName)) {
			CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();
			s3Client.createBucket(createBucketRequest);
		}
	}

	/**
	 * 判断bucket是否存在
	 * @param bucketName bucket名称
	 * @return 是否存在
	 */
	public boolean headBucket(String bucketName) {
		try {
			HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucketName).build();
			s3Client.headBucket(headBucketRequest);
			return true;
		}
		catch (NoSuchBucketException e) {
			return false;
		}
	}

	/**
	 * 获取全部bucket
	 * <p>
	 *
	 * @see <a href=
	 * "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ListBuckets">AWS API
	 * Documentation</a>
	 */
	public List<Bucket> getAllBuckets() {
		ListBucketsResponse listBucketsResponse = s3Client.listBuckets();
		return listBucketsResponse.buckets();
	}

	/**
	 * @param bucketName bucket名称
	 * @see <a href=
	 * "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ListBuckets">AWS API
	 * Documentation</a>
	 */
	public Optional<Bucket> getBucket(String bucketName) {
		return getAllBuckets().stream().filter(b -> b.name().equals(bucketName)).findFirst();
	}

	/**
	 * @param bucketName bucket名称
	 * @see <a href=
	 * "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/DeleteBucket">AWS API
	 * Documentation</a>
	 */
	public void removeBucket(String bucketName) {
		DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
		s3Client.deleteBucket(deleteBucketRequest);
	}

	/**
	 * 根据文件前置查询文件
	 * @param bucketName bucket名称
	 * @param prefix 前缀
	 * @see <a href=
	 * "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ListObjects">AWS API
	 * Documentation</a>
	 */
	public List<S3Object> getAllObjectsByPrefix(String bucketName, String prefix) {
		ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix)
				.build();

		ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
		return listObjectsResponse.contents();
	}

	/**
	 * 获取文件外链，只用于下载
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param minutes 过期时间，单位分钟,请注意该值必须小于7天
	 * @return url
	 */
	public String getObjectURL(String bucketName, String objectName, int minutes) {
		return getObjectURL(bucketName, objectName, Duration.ofMinutes(minutes));
	}

	/**
	 * 获取文件外链，只用于下载
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param expires 过期时间,请注意该值必须小于7天
	 * @return url
	 */
	public String getObjectURL(String bucketName, String objectName, Duration expires) {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(objectName).build();

		GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder().signatureDuration(expires)
				.getObjectRequest(getObjectRequest).build();

		PresignedGetObjectRequest presignedGetObjectRequest = s3Presigner.presignGetObject(getObjectPresignRequest);
		return presignedGetObjectRequest.url().toString();
	}

	/**
	 * 获取文件上传外链，只用于上传
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param minutes 过期时间，单位分钟,请注意该值必须小于7天
	 * @return url
	 */
	public String getPutObjectURL(String bucketName, String objectName, int minutes) {
		return getPutObjectURL(bucketName, objectName, Duration.ofMinutes(minutes));
	}

	/**
	 * 获取文件上传外链，只用于上传
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param expires 过期时间,请注意该值必须小于7天
	 * @return url
	 */
	public String getPutObjectURL(String bucketName, String objectName, Duration expires) {
		PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(objectName).build();

		PutObjectPresignRequest putObjectPresignRequest = PutObjectPresignRequest.builder().signatureDuration(expires)
				.putObjectRequest(putObjectRequest).build();

		PresignedPutObjectRequest presignedPutObjectRequest = s3Presigner.presignPutObject(putObjectPresignRequest);
		return presignedPutObjectRequest.url().toString();
	}

	/**
	 * 获取文件外链（兼容v1 API的方法签名）
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param minutes 过期时间，单位分钟,请注意该值必须小于7天
	 * @param httpMethod 请求方法（GET/PUT）
	 * @return url
	 */
	public String getObjectURL(String bucketName, String objectName, int minutes, String httpMethod) {
		return getObjectURL(bucketName, objectName, Duration.ofMinutes(minutes), httpMethod);
	}

	/**
	 * 获取文件外链
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param expires 过期时间，请注意该值必须小于7天
	 * @param httpMethod 请求方法（GET/PUT）
	 * @return url
	 */
	public String getObjectURL(String bucketName, String objectName, Duration expires, String httpMethod) {
		if ("PUT".equalsIgnoreCase(httpMethod)) {
			return getPutObjectURL(bucketName, objectName, expires);
		}
		else {
			return getObjectURL(bucketName, objectName, expires);
		}
	}

	/**
	 * 获取文件URL（公共访问）
	 * <p>
	 * If the object identified by the given bucket and key has public read permissions
	 * then this URL can be directly accessed to retrieve the object's data.
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @return url
	 */
	public String getObjectURL(String bucketName, String objectName) {
		return String.format("%s/%s/%s", ossProperties.getEndpoint(), bucketName, objectName);
	}

	/**
	 * 获取文件
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @return 二进制流
	 * @see <a href= "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/GetObject">AWS
	 * API Documentation</a>
	 */
	public InputStream getObject(String bucketName, String objectName) {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(objectName).build();

		return s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
	}

	/**
	 * 上传文件
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param stream 文件流
	 * @throws IOException IOException
	 */
	public void putObject(String bucketName, String objectName, InputStream stream) throws IOException {
		putObject(bucketName, objectName, stream, stream.available(), "application/octet-stream");
	}

	/**
	 * 上传文件 指定 contextType
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param stream 文件流
	 * @param contextType 文件类型
	 * @throws IOException IOException
	 */
	public void putObject(String bucketName, String objectName, String contextType, InputStream stream)
			throws IOException {
		putObject(bucketName, objectName, stream, stream.available(), contextType);
	}

	/**
	 * 上传文件
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param stream 文件流
	 * @param size 大小
	 * @param contextType 类型
	 * @see <a href= "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/PutObject">AWS
	 * API Documentation</a>
	 */
	public PutObjectResponse putObject(String bucketName, String objectName, InputStream stream, long size,
			String contextType) {
		PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(objectName)
				.contentType(contextType).contentLength(size).build();

		return s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(stream, size));
	}

	/**
	 * 获取文件信息
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @return 文件头信息
	 * @see <a href= "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/HeadObject">AWS
	 * API Documentation</a>
	 */
	public HeadObjectResponse getObjectInfo(String bucketName, String objectName) {
		HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucketName).key(objectName).build();

		return s3Client.headObject(headObjectRequest);
	}

	/**
	 * 删除文件
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @see <a href=
	 * "http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/DeleteObject">AWS API
	 * Documentation</a>
	 */
	public void removeObject(String bucketName, String objectName) {
		DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(objectName)
				.build();

		s3Client.deleteObject(deleteObjectRequest);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// 创建 S3 客户端
		this.s3Client = S3Client.builder().endpointOverride(URI.create(ossProperties.getEndpoint()))
				.region(Region.of(ossProperties.getRegion() != null ? ossProperties.getRegion() : "us-east-1"))
				.credentialsProvider(StaticCredentialsProvider
						.create(AwsBasicCredentials.create(ossProperties.getAccessKey(), ossProperties.getSecretKey())))
				.serviceConfiguration(
						S3Configuration.builder().pathStyleAccessEnabled(ossProperties.getPathStyleAccess())
								.chunkedEncodingEnabled(ossProperties.getChunkedEncodingEnabled()).build())
				.build();

		// 创建 S3 Presigner
		this.s3Presigner = S3Presigner.builder().endpointOverride(URI.create(ossProperties.getEndpoint()))
				.region(Region.of(ossProperties.getRegion() != null ? ossProperties.getRegion() : "us-east-1"))
				.credentialsProvider(StaticCredentialsProvider
						.create(AwsBasicCredentials.create(ossProperties.getAccessKey(), ossProperties.getSecretKey())))
				.serviceConfiguration(
						S3Configuration.builder().pathStyleAccessEnabled(ossProperties.getPathStyleAccess()).build())
				.build();
	}

}
