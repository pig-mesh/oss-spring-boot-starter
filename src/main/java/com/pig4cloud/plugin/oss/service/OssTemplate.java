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
import lombok.SneakyThrows;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * aws-s3 通用存储操作 支持所有兼容s3协议的云存储: {阿里云OSS，腾讯云COS，七牛云，京东云，minio 等}
 *
 * @author lengleng
 * @author lishangbu
 * @author 858695266
 * @date 2020/5/23 6:36 上午
 * @since 1.0
 */
@RequiredArgsConstructor
public class OssTemplate implements InitializingBean, DisposableBean {

	/**
	 * 对象存储服务配置
	 */
	private final OssProperties ossProperties;

	/**
	 * S3客户端
	 */
	private S3Client s3Client;

	/**
	 * S3 工具类
	 */
	private S3Utilities s3Utilities;

	/**
	 * S3预签名工具
	 */
	private S3Presigner s3Presigner;

	/**
	 * 创建bucket
	 *
	 * @param bucket 存储桶名称
	 * @return 文件服务器返回的创建存储桶的响应结果
	 * @throws BucketAlreadyExistsException     请求的存储桶名称不可用。存储桶名称空间由系统的所有用户共享。请选择其他名称然后重试。
	 * @throws BucketAlreadyOwnedByYouException 您尝试创建的存储桶已经存在，并且您拥有它。 Amazon
	 *                                          S3在除北弗吉尼亚州以外的所有AWS地区均返回此错误。为了实现旧兼容性，如果您重新创建在北弗吉尼亚州已经拥有的现有存储桶， Amazon S3将返回200
	 *                                          OK并重置存储桶访问控制列表（ACL）
	 * @throws AwsServiceException              SDK可能引发的所有异常的基类（不论是服务端异常还是客户端异常）。可用于所有场景下的异常捕获。
	 * @throws SdkClientException               如果发生任何客户端错误，例如与IO相关的异常，无法获取凭据等,会抛出此异常
	 * @throws S3Exception                      所有服务端异常的基类。未知异常将作为此类型的实例抛出
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_CreateBucket.html">创建存储桶</a>
	 */
	public CreateBucketResponse createBucket(String bucket) throws BucketAlreadyExistsException,
		BucketAlreadyOwnedByYouException, AwsServiceException, SdkClientException, S3Exception {
		return s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
	}

	/**
	 * 获取当前认证用户持有的全部存储桶信息列表
	 * @return 当前认证用户持有的获取全部存储桶信息列表
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_ListBuckets.html">罗列存储桶</a>
	 */
	@SneakyThrows
	public List<Bucket> getAllBuckets() {
		return s3Client.listBuckets(ListBucketsRequest.builder().build()).buckets();
	}

	/**
	 * 获取当前认证用户下持有的指定名称的存储桶信息
	 * @param bucketName 存储痛名称名称
	 * @return 当前认证用户下持有的指定名称的存储桶信息, 可能不存在
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_ListBuckets.html">罗列存储桶</a>
	 */
	@SneakyThrows
	public Optional<Bucket> getBucket(String bucketName) {
		return getAllBuckets().stream().filter(bucket -> bucket.name().equals(bucketName)).findFirst();
	}

	/**
	 * 根据指定的名称删除存储桶
	 *
	 * @param bucket bucket名称
	 * @return 文件服务器返回的删除存储桶的响应结果
	 * @throws AwsServiceException SDK可能引发的所有异常的基类（不论是服务端异常还是客户端异常）。可用于所有场景下的异常捕获。
	 * @throws SdkClientException  如果发生任何客户端错误，例如与IO相关的异常，无法获取凭据等,会抛出此异常
	 * @throws S3Exception         所有服务端异常的基类。未知异常将作为此类型的实例抛出
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_DeleteBucket.html">删除存储桶</a>
	 */
	public DeleteBucketResponse removeBucket(String bucket)
			throws AwsServiceException, SdkClientException, S3Exception {
		return s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
	}

	/**
	 * 根据文件前缀查询文件列表
	 * @param bucketName 存储桶名称
	 * @param prefix 文件前缀
	 * @return 对象列表
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_ListObjects.html">罗列对象</a>
	 */
	@SneakyThrows
	public List<S3Object> getAllObjectsByPrefix(String bucketName, String prefix) {
		return s3Client.listObjects(ListObjectsRequest.builder().bucket(bucketName).prefix(prefix).build()).contents();
	}

	/**
	 * 获取文件外链
	 * @param bucketName bucket名称
	 * @param keyName 文件名称
	 * @param duration 过期时间
	 * @return url的文本表示
	 * @see <a href=
	 * "https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/s3/src/main/java/com/example/s3/GetObjectPresignedUrl.java">获取文件外链</a>
	 */
	public String getObjectURL(String bucketName, String keyName, Duration duration) {

		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(keyName).build();

		GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder().signatureDuration(duration)
				.getObjectRequest(getObjectRequest).build();

		PresignedGetObjectRequest presignedGetObjectRequest = s3Presigner.presignGetObject(getObjectPresignRequest);
		URL url = presignedGetObjectRequest.url();
		return url.toString();
	}

	/**
	 * 获取文件外链,10分钟后过期
	 * @param bucketName bucket名称
	 * @param keyName 文件名称
	 * @return url的文本表示
	 * @see <a href=
	 * "https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/s3/src/main/java/com/example/s3/GetObjectPresignedUrl.java">获取文件外链</a>
	 */
	public String getObjectURL(String bucketName, String keyName) {
		return getObjectURL(bucketName, keyName, Duration.ofMinutes(10));
	}

	/**
	 * 获取文件URL,需保证有访问权限
	 *
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @return url
	 */
	public String getURL(String bucketName, String objectName) {
		URL url = s3Utilities.getUrl(GetUrlRequest.builder().bucket(bucketName).key(objectName)
			.endpoint(URI.create(ossProperties.getEndpoint())).region(Region.of(ossProperties.getRegion()))
			.build());
		return url.toString();
	}

	/**
	 * 上传文件
	 *
	 * @param bucketName bucket名称
	 * @param objectName 文件名称
	 * @param stream     文件流
	 * @return 文件服务器针对上传对象操作的返回结果
	 * @throws AwsServiceException SDK可能引发的所有异常的基类（不论是服务端异常还是客户端异常）。可用于所有场景下的异常捕获。
	 * @throws SdkClientException  如果发生任何客户端错误，例如与IO相关的异常，无法获取凭据等,会抛出此异常
	 * @throws S3Exception         所有服务端异常的基类。未知异常将作为此类型的实例抛出
	 * @throws IOException         IO异常
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutObject.html">往存储桶中添加对象</a>
	 */
	public PutObjectResponse putObject(String bucketName, String objectName, InputStream stream)
		throws AwsServiceException, SdkClientException, S3Exception, IOException {
		return putObject(bucketName, objectName, stream, stream.available(), "application/octet-stream");
	}

	/**
	 * 上传文件
	 *
	 * @param bucketName  bucket名称
	 * @param objectName  文件名称
	 * @param stream      文件流
	 * @param size        大小
	 * @param contextType 类型
	 * @return 文件服务器针对上传对象操作的返回结果
	 * @throws AwsServiceException SDK可能引发的所有异常的基类（不论是服务端异常还是客户端异常）。可用于所有场景下的异常捕获。
	 * @throws SdkClientException  如果发生任何客户端错误，例如与IO相关的异常，无法获取凭据等,会抛出此异常
	 * @throws S3Exception         所有服务端异常的基类。未知异常将作为此类型的实例抛出
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutObject.html">往存储桶中添加对象</a>
	 */
	public PutObjectResponse putObject(String bucketName, String objectName, InputStream stream, long size,
									   String contextType) throws AwsServiceException, SdkClientException, S3Exception {
		return s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(objectName).contentType(contextType)
			.contentLength(size).build(), RequestBody.fromInputStream(stream, size));
	}

	/**
	 * 根据指定的文件存储桶名称和对象名称获取对象信息
	 *
	 * @param bucket bucket名称
	 * @param key    文件名称
	 * @return S3对象
	 * @throws NoSuchKeyException          指定的文件名称不存在
	 * @throws InvalidObjectStateException 对象已存档，并且在归档状态还原之前不可访问
	 * @throws AwsServiceException         SDK可能引发的所有异常的基类（不论是服务端异常还是客户端异常）。可用于所有场景下的异常捕获。
	 * @throws SdkClientException          如果发生任何客户端错误，例如与IO相关的异常，无法获取凭据等,会抛出此异常
	 * @throws S3Exception                 所有服务端异常的基类。未知异常将作为此类型的实例抛出
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_GetObject.html">获取对象</a>
	 */
	public S3Object getObject(String bucket, String key) throws NoSuchKeyException, InvalidObjectStateException,
		AwsServiceException, SdkClientException, S3Exception {
		GetObjectResponse response = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
			.response();
		return S3Object.builder().key(key).eTag(response.eTag()).lastModified(response.lastModified())
			.size(response.contentLength()).storageClass(response.storageClassAsString()).build();

	}

	/**
	 * 删除对象
	 * @param bucket 存储桶名称
	 * @param key 文件名称
	 * @return 文件服务器返回的删除对象的响应结果
	 * @throws AwsServiceException SDK可能引发的所有异常的基类（不论是服务端异常还是客户端异常）。可用于所有场景下的异常捕获。
	 * @throws SdkClientException 如果发生任何客户端错误，例如与IO相关的异常，无法获取凭据等,会抛出此异常
	 * @throws S3Exception 所有服务端异常的基类。未知异常将作为此类型的实例抛出
	 * @see <a href=
	 * "https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_DeleteObject.html">删除对象</a>
	 */
	public DeleteObjectResponse removeObject(String bucket, String key)
		throws AwsServiceException, SdkClientException, S3Exception {
		return s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// 构造S3客户端
		AwsCredentials awsCredentials = AwsBasicCredentials.create(ossProperties.getAccessKey(),
				ossProperties.getSecretKey());
		AwsCredentialsProvider awsCredentialsProvider = StaticCredentialsProvider.create(awsCredentials);
		this.s3Client = S3Client.builder().credentialsProvider(awsCredentialsProvider)
				.region(Region.of(ossProperties.getRegion())).endpointOverride(URI.create(ossProperties.getEndpoint()))
				.build();
		// 构造S3工具类
		this.s3Utilities = S3Utilities.builder().region(Region.of(ossProperties.getRegion()))
				.s3Configuration(
						S3Configuration.builder().pathStyleAccessEnabled(ossProperties.getPathStyleAccess()).build())
				.build();
		// 构建预签名工具
		this.s3Presigner = S3Presigner.builder().region(Region.of(ossProperties.getRegion()))
				.endpointOverride(URI.create(ossProperties.getEndpoint())).credentialsProvider(awsCredentialsProvider)
				.build();
	}

	@Override
	public void destroy() throws Exception {
		this.s3Client.close();
		this.s3Presigner.close();
	}

}
