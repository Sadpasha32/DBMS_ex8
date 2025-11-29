package dbms.s3.demo.service;

import dbms.s3.demo.model.FileMetadata;
import org.springframework.web.multipart.MultipartFile;

import java.net.URL;
import java.util.List;

public interface S3FileService {

    List<FileMetadata> listFiles(String apiKey);

    FileMetadata upload(String apiKey, MultipartFile file);

    FileMetadata getMetadata(String apiKey, String fileId);

    void delete(String apiKey, String fileId);

    URL createPresignedUrl(String apiKey, String fileId, long expiresInSeconds);
}

