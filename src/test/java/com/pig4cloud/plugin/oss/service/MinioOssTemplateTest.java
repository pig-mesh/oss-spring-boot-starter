package com.pig4cloud.plugin.oss.service;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.ResourceUtils;

import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;

/**
 * minio模版测试
 *
 * @author lishangbu
 * @date 2021/1/25
 */
@SpringBootApplication(scanBasePackages = "com.pig4cloud.plugin.oss")
@SpringBootTest
@ActiveProfiles("minio")
public class MinioOssTemplateTest {

	/**
	 * 测试用OSS名字
	 */
	private static final String TEST_BUCKET_NAME = "test-oss";

	/**
	 * 测试用文件名,该文件在测试资源文件夹下
	 */
	private static final String TEST_OBJECT_NAME = "test.txt";

	/**
	 * 测试上传用文件名,该文件在测试资源文件夹下
	 */
	private static final String TEST_UPLOAD_OBJECT_NAME = "testUpload.txt";

	@Autowired
	private OssTemplate ossTemplate;

	@BeforeEach
	@SneakyThrows
	public void init() {
		ossTemplate.createBucket(TEST_BUCKET_NAME);
		ossTemplate.putObject(TEST_BUCKET_NAME, TEST_OBJECT_NAME,
			new FileInputStream(ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX + TEST_OBJECT_NAME)));

	}

	/**
	 * 测试获取存储桶
	 */
	@Test
	public void getBucket() {
		Optional<Bucket> bucket = ossTemplate.getBucket(TEST_BUCKET_NAME);
		Assertions.assertEquals(TEST_BUCKET_NAME, bucket.get().getName());
	}

	/**
	 * 获取对象
	 */
	@Test
	@SneakyThrows
	public void getObject() {
		S3Object s3Object = ossTemplate.getObject(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
		Assertions.assertEquals(TEST_BUCKET_NAME, s3Object.getBucketName());
		Assertions.assertEquals(TEST_OBJECT_NAME, s3Object.getKey());
		String content = IOUtils.toString(s3Object.getObjectContent().getDelegateStream());
		// 断言返回的文本包含文件的内容
		Assertions.assertTrue(content.contains("Hello,World!"));
	}

	/**
	 * 获取对象URL
	 */
	@Test
	public void getObjectUrl() {
		String url = ossTemplate.getObjectURL(TEST_BUCKET_NAME, TEST_OBJECT_NAME, 3);
		// 断言生成的链接必定包含过期时间字段
		Assertions.assertTrue(url.contains("X-Amz-Expires"));
	}

	@AfterEach
	@SneakyThrows
	public void destroy() {
		ossTemplate.removeObject(TEST_BUCKET_NAME, TEST_UPLOAD_OBJECT_NAME);
		ossTemplate.removeObject(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
		ossTemplate.removeBucket(TEST_BUCKET_NAME);
		Optional<Bucket> afterDeleteBucket = ossTemplate.getBucket(TEST_BUCKET_NAME);
		Assertions.assertEquals(Optional.empty(), afterDeleteBucket);
	}

	@Test
	@SneakyThrows
	public void getObjectUpload() {
		String testObjectContent = "it is a png";
		String url = ossTemplate.getObjectURL(TEST_BUCKET_NAME, TEST_UPLOAD_OBJECT_NAME, 1, HttpMethod.PUT);
		// 断言生成的链接必定包含过期时间字段
		Assertions.assertTrue(url.contains("X-Amz-Expires"));
		System.out.println("URL: " + url);

		Assertions.assertThrows(Exception.class, () -> ossTemplate.getObject(TEST_BUCKET_NAME, TEST_UPLOAD_OBJECT_NAME));
		Assertions.assertEquals(200, upload(url, testObjectContent));

		S3Object s3Object = ossTemplate.getObject(TEST_BUCKET_NAME, TEST_UPLOAD_OBJECT_NAME);
		Assertions.assertEquals(TEST_BUCKET_NAME, s3Object.getBucketName());
		Assertions.assertEquals(TEST_UPLOAD_OBJECT_NAME, s3Object.getKey());
		String content = IOUtils.toString(s3Object.getObjectContent().getDelegateStream());
		// 断言返回的文本包含文件的内容
		Assertions.assertTrue(content.contains(testObjectContent));
	}

	@Test
	@SneakyThrows
	public void getObjectUploadExpired() {
		String testObjectContent = "it is another png";
		String url = ossTemplate.getObjectURL(TEST_BUCKET_NAME, TEST_UPLOAD_OBJECT_NAME, 1, HttpMethod.PUT);
		// 断言生成的链接必定包含过期时间字段
		Assertions.assertTrue(url.contains("X-Amz-Expires"));
		System.out.println("URL: " + url);

		Assertions.assertThrows(Exception.class, () -> ossTemplate.getObject(TEST_BUCKET_NAME, TEST_UPLOAD_OBJECT_NAME));

		Thread.sleep(1100 * 60);
		Assertions.assertEquals(403, upload(url, testObjectContent));

		Assertions.assertThrows(Exception.class, () -> ossTemplate.getObject(TEST_BUCKET_NAME, TEST_UPLOAD_OBJECT_NAME));
	}

	@SneakyThrows
	private int upload(String url, String content) {
		// Create the connection and use it to upload the new object using the pre-signed URL.
		URL opurl = new URL(url);
		HttpURLConnection connection = (HttpURLConnection) opurl.openConnection();
		connection.setDoOutput(true);
		connection.setRequestMethod("PUT");
		OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
		out.write(content);
		out.close();

		// Check the HTTP response code. To complete the upload and make the object available,
		// you must interact with the connection object in some way.
		System.out.println("HTTP response code: " + connection.getResponseCode());
		return connection.getResponseCode();
	}
}
