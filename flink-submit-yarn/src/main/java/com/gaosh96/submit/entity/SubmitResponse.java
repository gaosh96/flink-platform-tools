package com.gaosh96.submit.entity;

import lombok.Builder;
import lombok.Data;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/11
 */
@Data
@Builder
public class SubmitResponse {

    private String status;
    private String jobId;
    private String applicationId;
    private String jobManagerUrl;

}
