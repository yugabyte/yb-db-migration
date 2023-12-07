/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
 /*
CHANGES TO THIS FILE ARE ONLY NECESSARY WHEN DEALING WITH ANY API CHANGES FROM DEBEZIUM.
SEPARATE FILES FOR DEBEZIUM VERSIONS 2.X AND 1.X WERE CREATED TO DEAL WITH BREAKING CHANGES

THE DIFFERENCES BETWEEN 1X AND 2X FILES ARE:
    -  https://debezium.io/releases/2.2/release-notes#breaking_changes_3
        - import javax.* -> import jakarta.*
THE POM FILES ARE RESPONSIBLE FOR INCLUDING THE APPROPRIATE FILE.
 */
package io.debezium.server.ybexporter;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Implementation of the consumer that exports the messages to file in a Yugabyte-compatible form.
 */
@Named("ybexporter")
@Dependent
public class YbExporterConsumer1x extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YbExporterConsumer1x.class);
    private static final String PROP_PREFIX = "debezium.sink.ybexporter.";
    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    private YbExporterConsumer ybec;

    @PostConstruct
    void connect() throws URISyntaxException {
        ybec = new YbExporterConsumer(dataDir);
        ybec.connect();
    }



    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> changeEvents, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        ybec.handleBatch(changeEvents, committer);
    }
}
