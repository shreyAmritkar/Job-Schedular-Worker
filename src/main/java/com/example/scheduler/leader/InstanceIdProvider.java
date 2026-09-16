package com.example.scheduler.leader;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Generates exactly one instanceId per JVM, at startup, and hands it out to
 * every leader-election collaborator. Format is "{hostname}-{shortUuid}":
 * the hostname makes logs/Redis values readable for humans debugging which
 * physical/container instance holds the lease; the UUID suffix guarantees
 * uniqueness even when two instances share a hostname (e.g. same container
 * image, different replicas without a stable pod name available).
 */
@Component
public class InstanceIdProvider {

    private final String instanceId;

    public InstanceIdProvider() {
        this.instanceId = resolveHostname() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-host";
        }
    }

    public String getInstanceId() {
        return instanceId;
    }
}
