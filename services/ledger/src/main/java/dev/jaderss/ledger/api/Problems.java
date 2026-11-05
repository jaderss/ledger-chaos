package dev.jaderss.ledger.api;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

final class Problems {

    private Problems() {}

    static ProblemDetail of(HttpStatus status, String slug, String detail) {
        var problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create("/problems/" + slug));
        problem.setTitle(slug.replace('-', ' '));
        return problem;
    }
}
