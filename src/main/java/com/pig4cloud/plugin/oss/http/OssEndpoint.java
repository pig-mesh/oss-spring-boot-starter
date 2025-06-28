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

package com.pig4cloud.plugin.oss.http;

import com.pig4cloud.plugin.oss.service.OssTemplate;
import io.swagger.annotations.Api;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import net.dreamlu.mica.auto.annotation.AutoIgnore;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * oss 对外提供服务端点
 *
 * @author lengleng
 * @author 858695266
 * <p>
 * oss.info
 */
@Validated
@AutoIgnore
@RestController
@RequiredArgsConstructor
@RequestMapping("${oss.http.prefix:}/oss")
@Api(tags = "oss:http接口")
@Tag(name = "OssEndpoint", description = "oss:http接口")
public class OssEndpoint {

	/**
	 * OSS操作模板
	 */
	private final OssTemplate ossTemplate;

	/**
	 * Bucket Endpoints
	 */
	@PostMapping("/bucket/{bucketName}")
	public Bucket createBucket(@PathVariable @NotBlank String bucketName) {
		ossTemplate.createBucket(bucketName);
		return ossTemplate.getBucket(bucketName).get();
	}

	@GetMapping("/bucket")
	public List<Bucket> getBuckets() {
		return ossTemplate.getAllBuckets();
	}

	@GetMapping("/bucket/{bucketName}")
	public Bucket getBucket(@PathVariable @NotBlank String bucketName) {
		return ossTemplate.getBucket(bucketName)
				.orElseThrow(() -> new IllegalArgumentException("Bucket Name not found!"));
	}

	@DeleteMapping("/bucket/{bucketName}")
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void deleteBucket(@PathVariable @NotBlank String bucketName) {
		ossTemplate.removeBucket(bucketName);
	}

	/**
	 * Object Endpoints
	 */
	@SneakyThrows
	@PostMapping("/object/{bucketName}")
	public Map<String, Object> createObject(@RequestBody @NotNull MultipartFile object,
			@PathVariable @NotBlank String bucketName) {
		@Cleanup
		InputStream inputStream = object.getInputStream();
		String name = object.getOriginalFilename();

		ossTemplate.putObject(bucketName, name, inputStream, object.getSize(), object.getContentType());
		HeadObjectResponse objectInfo = ossTemplate.getObjectInfo(bucketName, name);

		Map<String, Object> result = new HashMap<>();
		result.put("key", name);
		result.put("bucketName", bucketName);
		result.put("eTag", objectInfo.eTag());
		result.put("lastModified", objectInfo.lastModified());
		result.put("size", objectInfo.contentLength());
		return result;
	}

	/**
	 * Object Endpoints
	 */
	@SneakyThrows
	@PostMapping("/object/{bucketName}/{objectName}")
	public Map<String, Object> createObject(@RequestBody @NotNull MultipartFile object,
			@PathVariable @NotBlank String bucketName, @PathVariable @NotBlank String objectName) {
		@Cleanup
		InputStream inputStream = object.getInputStream();
		ossTemplate.putObject(bucketName, objectName, inputStream, object.getSize(), object.getContentType());
		HeadObjectResponse objectInfo = ossTemplate.getObjectInfo(bucketName, objectName);

		Map<String, Object> result = new HashMap<>();
		result.put("key", objectName);
		result.put("bucketName", bucketName);
		result.put("eTag", objectInfo.eTag());
		result.put("lastModified", objectInfo.lastModified());
		result.put("size", objectInfo.contentLength());
		return result;
	}

	@GetMapping("/object/{bucketName}/{objectName}")
	public List<S3Object> filterObject(@PathVariable @NotBlank String bucketName,
			@PathVariable @NotBlank String objectName) {
		return ossTemplate.getAllObjectsByPrefix(bucketName, objectName);
	}

	@GetMapping("/object/{bucketName}/{objectName}/{expires}")
	public Map<String, Object> getObjectUrl(@PathVariable @NotBlank String bucketName,
			@PathVariable @NotBlank String objectName, @PathVariable @NotNull Integer expires) {
		Map<String, Object> responseBody = new HashMap<>(8);
		// Put Object info
		responseBody.put("bucket", bucketName);
		responseBody.put("object", objectName);
		responseBody.put("url", ossTemplate.getObjectURL(bucketName, objectName, expires));
		responseBody.put("expires", expires);
		return responseBody;
	}

	@GetMapping("/object/put/{bucketName}/{objectName}/{expires}")
	public Map<String, Object> getPutObjectUrl(@PathVariable @NotBlank String bucketName,
			@PathVariable @NotBlank String objectName, @PathVariable @NotNull Integer expires) {
		Map<String, Object> responseBody = new HashMap<>(8);
		// Put Object info
		responseBody.put("bucket", bucketName);
		responseBody.put("object", objectName);
		responseBody.put("url", ossTemplate.getPutObjectURL(bucketName, objectName, expires));
		responseBody.put("expires", expires);
		return responseBody;
	}

	@ResponseStatus(HttpStatus.ACCEPTED)
	@DeleteMapping("/object/{bucketName}/{objectName}/")
	public void deleteObject(@PathVariable @NotBlank String bucketName, @PathVariable @NotBlank String objectName) {
		ossTemplate.removeObject(bucketName, objectName);
	}

}
