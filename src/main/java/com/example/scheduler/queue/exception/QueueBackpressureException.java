package com.example.scheduler.queue.exception;

public class QueueBackpressureException extends RuntimeException {

    public QueueBackpressureException(String message) {
        super(message);
    }
}
