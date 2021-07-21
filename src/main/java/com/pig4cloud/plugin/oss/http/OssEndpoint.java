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

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.pig4cloud.plugin.oss.service.OssTemplate;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import net.dreamlu.mica.auto.annotation.AutoIgnore;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
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
	public S3Object createObject(@RequestBody @NotNull MultipartFile object,
			@PathVariable @NotBlank String bucketName) {
		String name = object.getOriginalFilename();
		ossTemplate.putObject(bucketName, name, object.getInputStream(), object.getSize(), object.getContentType());
		return ossTemplate.getObjectInfo(bucketName, name);

	}

	@SneakyThrows
	@PostMapping("/object/{bucketName}/{objectName}")
	public S3Object createObject(@RequestBody @NotNull MultipartFile object, @PathVariable @NotBlank String bucketName,
			@PathVariable @NotBlank String objectName) {
		ossTemplate.putObject(bucketName, objectName, object.getInputStream(), object.getSize(),
				object.getContentType());
		return ossTemplate.getObjectInfo(bucketName, objectName);
	}

	@GetMapping("/object/{bucketName}/{objectName}")
	public List<S3ObjectSummary> filterObject(@PathVariable @NotBlank String bucketName,
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
