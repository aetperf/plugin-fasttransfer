package io.kestra.plugin.fasttransfer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * This test will only test the main task, this allows you to send any input
 * parameters to your task and test the returning behaviour easily.
 */
@KestraTest
class FastTransferTest {
    @Inject
    private RunContextFactory runContextFactory;
/*
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("variable", "John Doe"));

        FastTransfer task = FastTransfer.builder()
            .format(new Property<>("Hello {{ variable }}"))
            .build();

        FastTransfer.Output runOutput = task.run(runContext);

        assertThat(runOutput.getChild().getValue(), is(StringUtils.reverse("Hello John Doe")));
    }*/
}
