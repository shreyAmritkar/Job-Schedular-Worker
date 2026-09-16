package com.example.scheduler.common;

import org.springframework.data.domain.Page;

import java.util.List;

/**
 * Deliberately not returning Spring Data's Page<T> directly from the
 * controller: its default JSON shape leaks Pageable/Sort internals and
 * isn't a contract we want to be locked into. This is the stable public
 * shape instead.
 */
public record PageResponse<T>(
        List<T> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
    public static <T> PageResponse<T> from(Page<T> page) {
        return new PageResponse<>(
                page.getContent(),
                page.getNumber(),
                page.getSize(),
                page.getTotalElements(),
                page.getTotalPages()
        );
    }
}
