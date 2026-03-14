package com.acme.messaging.kafka.core.helpers;

import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
@Slf4j
public class StringHelper {

    public static String buildUUID() {

        String uuid = UUID.randomUUID().toString();

        log.info("UUID built [{}].", uuid);

        return uuid;
    }
}
