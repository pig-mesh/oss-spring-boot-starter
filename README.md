## oss-spring-boot-starter

[![Maven Central](https://img.shields.io/maven-central/v/com.pig4cloud.plugin/oss-spring-boot-starter.svg)](https://search.maven.org/artifact/com.pig4cloud.plugin/oss-spring-boot-starter)

兼容S3 协议的通用文件存储工具类 ，支持 兼容S3 协议的云存储 

- MINIO
- 阿里云
- 华为云
- 腾讯云
- 京东云

...

## spring boot starter依赖


| 版本 | 支持 |
|-------|--|
| 3.2.0 | 适配 SpringBoot3.x |
| 1.0.5 | 适配 SpringBoot2.x |

- 方便在 web 环境下使用 `oss` ，已上传至 maven 仓库
```xml
<dependency>
    <groupId>com.pig4cloud.plugin</groupId>
    <artifactId>oss-spring-boot-starter</artifactId>
    <version>${lastVersion}</version>
</dependency>
```

## 使用方法

### 配置文件

```yaml
oss:
  endpoint: http://minio.pig4cloud.com
  access-key: lengleng
  secret-key: lengleng
```

### 代码使用

```java
@Autowired
private OssTemplate template;
/**
 * 上传文件
 * 文件名采用uuid,避免原始文件名中带"-"符号导致下载的时候解析出现异常
 *
 * @param file 资源
 * @return R(bucketName, filename)
 */
@PostMapping("/upload")
public R upload(@RequestParam("file") MultipartFile file, HttpServletRequest request) {
	template.putObject(CommonConstants.BUCKET_NAME, fileName, file.getInputStream());
	return R.ok(resultMap);
}
```
