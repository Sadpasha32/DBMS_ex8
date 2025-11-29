package dbms.s3.demo.service;

import dbms.s3.demo.model.FileMetadata;
import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.Item;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class MinioS3FileService implements S3FileService {

    private final MinioClient minioClient;
    private final String bucket = "file-exchange-service";

    private String prefix(String apiKey) {
        return "files/" + apiKey + "/";
    }

    private String objectName(String apiKey, String fileId) {
        return prefix(apiKey) + fileId;
    }

    @Override
    public List<FileMetadata> listFiles(String apiKey) {
        List<FileMetadata> result = new ArrayList<>();
        try {
            Iterable<Result<Item>> objects = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucket)
                            .prefix(prefix(apiKey))
                            .recursive(true)
                            .build());
            for (Result<Item> r : objects) {
                Item item = r.get();
                String id = item.objectName().substring(prefix(apiKey).length());
                result.add(FileMetadata.builder()
                        .id(id)
                        .filename(item.objectName())
                        .size(item.size())
                        .createdAt(item.lastModified().toInstant())
                        .build());
            }
            log.info("MinIO list: user={}, objects={}", apiKey, result.size());
        } catch (Exception e) {
            log.error("MinIO list failed: user={}, error={}", apiKey, e.getMessage(), e);
            throw new RuntimeException("Cannot list objects", e);
        }
        return result;
    }

    @Override
    public FileMetadata upload(String apiKey, MultipartFile file) {
        String objectName = prefix(apiKey) + file.getOriginalFilename();
        log.info("MinIO upload: user={}, object={}", apiKey, objectName);
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
            if (!found) {
                log.info("MinIO bucket missing, creating: {}", bucket);
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
            }

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(objectName)
                            .stream(file.getInputStream(), file.getSize(), -1)
                            .contentType(file.getContentType())
                            .build());

            return FileMetadata.builder()
                    .id(file.getOriginalFilename())
                    .filename(file.getOriginalFilename())
                    .size(file.getSize())
                    .createdAt(Instant.now())
                    .build();
        } catch (Exception e) {
            log.error("MinIO upload failed: user={}, object={}, error={}",
                    apiKey, objectName, e.getMessage(), e);
            throw new RuntimeException("Cannot upload file", e);
        }
    }

    @Override
    public FileMetadata getMetadata(String apiKey, String fileId) {
        String objectName = objectName(apiKey, fileId);
        try {
            StatObjectResponse stat = minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucket)
                            .object(objectName)
                            .build());
            return FileMetadata.builder()
                    .id(fileId)
                    .filename(stat.object())
                    .size(stat.size())
                    .createdAt(stat.lastModified().toInstant())
                    .build();
        } catch (ErrorResponseException e) {
            if ("NoSuchKey".equals(e.errorResponse().code())) {
                log.warn("MinIO stat: object not found, user={}, fileId={}", apiKey, fileId);
                return null;
            }
            log.error("MinIO stat failed: user={}, fileId={}, error={}", apiKey, fileId, e.getMessage(), e);
            throw new RuntimeException("Cannot stat object", e);
        } catch (Exception e) {
            log.error("MinIO stat failed: user={}, fileId={}, error={}", apiKey, fileId, e.getMessage(), e);
            throw new RuntimeException("Cannot stat object", e);
        }
    }

    @Override
    public void delete(String apiKey, String fileId) {
        String objectName = objectName(apiKey, fileId);
        log.info("MinIO delete: user={}, object={}", apiKey, objectName);
        try {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucket)
                    .object(objectName)
                    .build());
        } catch (Exception e) {
            log.error("MinIO delete failed: user={}, object={}, error={}", apiKey, objectName, e.getMessage(), e);
            throw new RuntimeException("Cannot delete object", e);
        }
    }

    @Override
    public URL createPresignedUrl(String apiKey, String fileId, long expiresInSeconds) {
        String objectName = objectName(apiKey, fileId);
        log.info("MinIO presign: user={}, object={}, expiresIn={}s", apiKey, objectName, expiresInSeconds);
        try {
            String url = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucket)
                            .object(objectName)
                            .expiry((int) expiresInSeconds, TimeUnit.SECONDS)
                            .build());
            return new URI(url).toURL();
        } catch (Exception e) {
            log.error("MinIO presign failed: user={}, object={}, error={}", apiKey, objectName, e.getMessage(), e);
            throw new RuntimeException("Cannot create presigned url", e);
        }
    }
}
