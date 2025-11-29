package dbms.s3.demo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadata {
    private String id;
    private String filename;
    private long size;
    private Instant createdAt;
}

