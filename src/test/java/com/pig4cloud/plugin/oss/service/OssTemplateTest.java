package com.pig4cloud.plugin.oss.service;

import com.pig4cloud.plugin.oss.OssProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.ResourceUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * oss操作模版测试
 *
 * @author lishangbu
 * @date 2020/12/23
 */
@SpringBootApplication(scanBasePackages = "com.pig4cloud.plugin.oss")
@SpringBootTest
public class OssTemplateTest extends AbstractTestNGSpringContextTests {

	/**
	 * 测试用OSS名字
	 */
	private static final String TEST_BUCKET_NAME = "test-oss";

	/**
	 * 测试用文件名,该文件在测试资源文件夹下
	 */
	private static final String TEST_OBJECT_NAME = "test.md";

	@Autowired
	private OssTemplate ossTemplate;

	@Autowired
	private OssProperties ossProperties;

	@Test(priority = Integer.MIN_VALUE)
	public void createBucket() {
		CreateBucketResponse bucket = ossTemplate.createBucket(TEST_BUCKET_NAME);
		Assert.assertEquals("/" + TEST_BUCKET_NAME, bucket.location());
	}

	/**
	 * 测试获取存储桶
	 */
	@Test(priority = 0)
	public void getBucket() {
		Optional<Bucket> bucket = ossTemplate.getBucket(TEST_BUCKET_NAME);
		Assert.assertEquals(bucket.get().name(), TEST_BUCKET_NAME);
	}

	/**
	 * 测试文件上传 上传后的文件名和当前文件名保持一致
	 */
	@Test(priority = 1)
	public void putObject() throws IOException {
		ossTemplate.putObject(TEST_BUCKET_NAME, TEST_OBJECT_NAME,
			new FileInputStream(ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX + TEST_OBJECT_NAME)));
	}

	/**
	 * 获取文件URL,如果要用这个url访问存储桶需要公开
	 */
	@Test(priority = 2)
	public void getUrl() {
		String url = ossTemplate.getURL(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
		Assert.assertEquals(ossProperties.getEndpoint() + "/" + TEST_BUCKET_NAME + "/" + TEST_OBJECT_NAME, url);
	}

	/**
	 * 获取对象
	 */
	@Test(priority = 3)
	public void getObject() {
		S3Object s3Object = ossTemplate.getObject(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
		Assert.assertEquals(s3Object.owner(), null);
		Assert.assertEquals(s3Object.key(), TEST_OBJECT_NAME);
	}

	/**
	 * 获取对象URL
	 */
	@Test(priority = 4)
	public void getObjectUrl() {
		String url = ossTemplate.getObjectURL(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
		// 断言生成的链接必定包含过期时间字段
		Assert.assertTrue(url.contains("X-Amz-Expires"));
	}

	/**
	 * 获取对象URL
	 */
	@Test(priority = 5)
	public void removeObject() {
		ossTemplate.removeObject(TEST_BUCKET_NAME, TEST_OBJECT_NAME);
	}

	@Test(priority = Integer.MAX_VALUE)
	public void deleteBucket() {
		ossTemplate.removeBucket(TEST_BUCKET_NAME);
		Optional<Bucket> afterDeleteBucket = ossTemplate.getBucket(TEST_BUCKET_NAME);
		Assert.assertEquals(Optional.empty(), afterDeleteBucket);
	}

}
