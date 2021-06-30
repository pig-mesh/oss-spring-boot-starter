## oss-spring-boot-starter

兼容S3 协议的通用文件存储工具类 ，支持 兼容S3 协议的云存储 

- MINIO
- 阿里云
- 华为云
- 腾讯云
- 京东云

...

## spring boot starter依赖

- 方便在 web 环境下使用 `oss` ，已上传至 maven 仓库
```xml
<dependency>
    <groupId>com.pig4cloud.plugin</groupId>
    <artifactId>oss-spring-boot-starter</artifactId>
    <version>0.0.6</version>
</dependency>
```

## 使用方法

### 配置文件

```
oss:
  #使用云OSS  需要关闭
  path-style-access: false 
  #对应上图 ③ 处配置
  endpoint: s3-cn-east-1.qiniucs.com 
  # 上文创建的AK, 一定注意复制完整不要有空格
  access-key: xxx   
  # 上文创建的SK, 一定注意复制完整不要有空格
  secret-key: xxx   
   # 上文创建的桶名称
  bucketName: pig4cloud 
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
