package dbms.s3.demo.controller;

import dbms.s3.demo.controller.dto.ShareRequest;
import dbms.s3.demo.controller.dto.ShareResponse;
import dbms.s3.demo.controller.dto.StatsResponse;
import dbms.s3.demo.model.FileMetadata;
import dbms.s3.demo.service.S3FileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.net.URL;
import java.util.List;


@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class FileController {

    private final S3FileService fileService;

    @PostMapping(value = "/files/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<FileMetadata> upload(@RequestHeader("X-API-Key") String apiKey,
                                               @RequestPart("file") MultipartFile file) {
        log.info("Upload request: user={}, filename={}, size={} bytes",
                apiKey, file.getOriginalFilename(), file.getSize());

        if (file.isEmpty()) {
            log.warn("Upload rejected: empty file for user={}", apiKey);
            return ResponseEntity.badRequest().build();
        }

        FileMetadata metadata = fileService.upload(apiKey, file);
        log.info("Upload success: user={}, fileId={}, size={} bytes",
                apiKey, metadata.getId(), metadata.getSize());
        return ResponseEntity.status(HttpStatus.CREATED).body(metadata);
    }

    @GetMapping("/files")
    public List<FileMetadata> list(@RequestHeader("X-API-Key") String apiKey) {
        List<FileMetadata> files = fileService.listFiles(apiKey);
        log.info("List files: user={}, count={}", apiKey, files.size());
        return files;
    }

    @GetMapping("/files/{fileId}")
    public ResponseEntity<FileMetadata> info(@RequestHeader("X-API-Key") String apiKey,
                                             @PathVariable String fileId) {
        FileMetadata meta = fileService.getMetadata(apiKey, fileId);
        if (meta == null) {
            log.warn("File not found: user={}, fileId={}", apiKey, fileId);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        log.info("Get file info: user={}, fileId={}, size={} bytes", apiKey, fileId, meta.getSize());
        return ResponseEntity.ok(meta);
    }

    @PostMapping("/files/{fileId}/share")
    public ResponseEntity<ShareResponse> share(
            @RequestHeader("X-API-Key") String apiKey,
            @PathVariable String fileId,
            @RequestBody(required = false) ShareRequest request) {

        long expiresIn = (request != null && request.getExpiresIn() != null)
                ? request.getExpiresIn()
                : 3600L;

        log.info("Create presigned URL: user={}, fileId={}, expiresIn={}s",
                apiKey, fileId, expiresIn);

        URL presignedUrl = fileService.createPresignedUrl(apiKey, fileId, expiresIn);

        ShareResponse response = ShareResponse.builder()
                .url(presignedUrl.toString())
                .expiresIn(expiresIn)
                .build();

        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/files/{fileId}")
    public ResponseEntity<Void> delete(@RequestHeader("X-API-Key") String apiKey,
                                       @PathVariable String fileId) {
        log.info("Delete file: user={}, fileId={}", apiKey, fileId);
        fileService.delete(apiKey, fileId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/stats")
    public ResponseEntity<StatsResponse> stats(@RequestHeader("X-API-Key") String apiKey) {
        List<FileMetadata> files = fileService.listFiles(apiKey);
        long totalSize = files.stream().mapToLong(FileMetadata::getSize).sum();

        log.info("Stats: user={}, filesCount={}, totalSize={} bytes", apiKey, files.size(), totalSize);

        return ResponseEntity.ok(StatsResponse.builder()
                .filesCount(files.size())
                .totalSize(totalSize)
                .build());
    }
}
