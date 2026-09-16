package com.example.scheduler.job.exception;

public class JobConflictException extends RuntimeException {

    public JobConflictException(String message) {
        super(message);
    }
}
