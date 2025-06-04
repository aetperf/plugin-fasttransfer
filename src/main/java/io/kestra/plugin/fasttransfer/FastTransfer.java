package io.kestra.plugin.fasttransfer;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Map.entry;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a FastTransfer data movement.",
    description = "This task runs the FastTransfer binary to move data between heterogeneous sources and targets using high-performance bulk operations. " +
        "It supports a variety of databases and configuration options for parallelism, authentication, and data transformation. " +
        "See [FastTransfer documentation](https://www.arpe.io/fasttransfer/?v=82a9e4d26595) for full parameter reference and usage."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Transfer a table using FastTransfer with license",
            code = {
                "sourceConnectionType: mssql",
                "sourceServer: localhost,11433",
                "sourceUser: FastTransfer_Login",
                "sourcePassword: FastPassword",
                "sourceDatabase: tpch10",
                "sourceSchema: dbo",
                "sourceTable: orders",
                "targetConnectionType: msbulk",
                "targetServer: localhost,31433",
                "targetUser: FastTransfer_Login",
                "targetPassword: FastPassword",
                "targetDatabase: tpch10",
                "targetSchema: dbo",
                "targetTable: orders2",
                "degree: 12",
                "method: Ntile",
                "distributeKeyColumn: o_orderkey",
                "loadMode: Truncate",
                "mapMethod: Position",
                "batchSize: 1048576",
                "useWorkTables: true",
                "license: YOUR_LICENSE_KEY"
            }
        )
    }
)

public class FastTransfer extends Task implements RunnableTask<FastTransfer.Output> {
    @Schema(title = "Source connection type", description = "Source connection type (e.g., mssql, pgsql, mysql, etc.)")
    private Property<String> sourceConnectionType;

    @Schema(title = "Source connect string", description = "Connection string for source")
    private Property<String> sourceConnectString;

    @Schema(title = "Source DSN", description = "ODBC Data Source Name")
    private Property<String> sourceDsn;

    @Schema(title = "Source provider", description = "OLE DB provider (e.g., MSOLEDBSQL)")
    private Property<String> sourceProvider;

    @Schema(title = "Source server", description = "Source SQL server address")
    private Property<String> sourceServer;

    @Schema(title = "Source user", description = "Username for source connection")
    private Property<String> sourceUser;

    @Schema(title = "Source password", description = "Password for source connection")
    private Property<String> sourcePassword;

    @Schema(title = "Source trusted", description = "Use trusted authentication for source")
    private Property<Boolean> sourceTrusted;

    @Schema(title = "Source database", description = "Source database name")
    private Property<String> sourceDatabase;

    @Schema(title = "Source schema", description = "Source schema name")
    private Property<String> sourceSchema;

    @Schema(title = "Source table", description = "Source table name")
    private Property<String> sourceTable;

    @Schema(title = "SQL query", description = "Plain SQL query to execute")
    private Property<String> query;

    @Schema(title = "File input", description = "File containing SQL query")
    private Property<String> fileInput;

    @Schema(title = "Target connection type", description = "Target connection type (e.g., pgcopy, mysqlbulk)")
    private Property<String> targetConnectionType;

    @Schema(title = "Target connect string", description = "Connection string for target")
    private Property<String> targetConnectString;

    @Schema(title = "Target server", description = "Target SQL server address")
    private Property<String> targetServer;

    @Schema(title = "Target user", description = "Username for target connection")
    private Property<String> targetUser;

    @Schema(title = "Target password", description = "Password for target connection")
    private Property<String> targetPassword;

    @Schema(title = "Target trusted", description = "Use trusted authentication for target")
    private Property<Boolean> targetTrusted;

    @Schema(title = "Target database", description = "Target database name")
    private Property<String> targetDatabase;

    @Schema(title = "Target schema", description = "Target schema name")
    private Property<String> targetSchema;

    @Schema(title = "Target table", description = "Target table name")
    private Property<String> targetTable;

    @Schema(title = "Degree of parallelism", description = "Degree of parallelism (0 = Auto)")
    private Property<Integer> degree;

    @Schema(title = "Parallel method", description = "Parallel split method (e.g., Random, DataDriven, None)")
    private Property<String> method;

    @Schema(title = "Distribute key column", description = "Column used to distribute data")
    private Property<String> distributeKeyColumn;

    @Schema(title = "Data driven query", description = "SQL query to retrieve data-driven values")
    private Property<String> dataDrivenQuery;

    @Schema(title = "Load mode", description = "Load mode (Append or Truncate)")
    private Property<String> loadMode;

    @Schema(title = "Batch size", description = "Batch size for bulk copy")
    private Property<Integer> batchSize;

    @Schema(title = "Use work tables", description = "Use intermediate work tables")
    private Property<Boolean> useWorkTables;

    @Schema(title = "Run ID", description = "Run identifier for logging")
    private Property<String> runId;

    @Schema(title = "Settings file", description = "Path to settings file")
    private Property<String> settingsFile;

    @Schema(title = "Column map method", description = "Mapping method for columns (Position or Name)")
    private Property<String> mapMethod;

    @Schema(title = "License file path or URL", description = "Path or URL of the license file. If not provided, FastTransfer will look for a local FastTransfer.lic file next to the binary.")
    private Property<String> license;


    @Override
    public FastTransfer.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Ressource binaire Linux uniquement
        String executableResource = "/FastTransfer";

        // Extraire le binaire dans un fichier temporaire
        File tempExe = File.createTempFile("fasttransfer-", null);
        tempExe.deleteOnExit();

        try (InputStream is = getClass().getResourceAsStream(executableResource);
             OutputStream os = new FileOutputStream(tempExe)) {
            if (is == null) {
                throw new IllegalStateException("Executable resource not found: " + executableResource);
            }
            byte[] buffer = new byte[8192];
            int read;
            while ((read = is.read(buffer)) != -1) {
                os.write(buffer, 0, read);
            }
        }

        boolean executableSet = tempExe.setExecutable(true);

        List<String> command = new ArrayList<>();
        List<String> commandLog = new ArrayList<>();
        command.add(tempExe.getAbsolutePath());
        commandLog.add(tempExe.getAbsolutePath());

        Map<String, Property<?>> params = new LinkedHashMap<>();
        params.put("--sourceconnectiontype", sourceConnectionType);
        params.put("--sourceconnectstring", sourceConnectString);
        params.put("--sourcedsn", sourceDsn);
        params.put("--sourceprovider", sourceProvider);
        params.put("--sourceserver", sourceServer);
        params.put("--sourceuser", sourceUser);
        params.put("--sourcepassword", sourcePassword);
        params.put("--sourcetrusted", sourceTrusted);
        params.put("--sourcedatabase", sourceDatabase);
        params.put("--sourceschema", sourceSchema);
        params.put("--sourcetable", sourceTable);
        params.put("--query", query);
        params.put("--fileinput", fileInput);
        params.put("--targetconnectiontype", targetConnectionType);
        params.put("--targetconnectstring", targetConnectString);
        params.put("--targetserver", targetServer);
        params.put("--targetuser", targetUser);
        params.put("--targetpassword", targetPassword);
        params.put("--targettrusted", targetTrusted);
        params.put("--targetdatabase", targetDatabase);
        params.put("--targetschema", targetSchema);
        params.put("--targettable", targetTable);
        params.put("--degree", degree);
        params.put("--method", method);
        params.put("--distributekeycolumn", distributeKeyColumn);
        params.put("--datadrivenquery", dataDrivenQuery);
        params.put("--loadmode", loadMode);
        params.put("--batchsize", batchSize);
        params.put("--useworktables", useWorkTables);
        params.put("--runid", runId);
        params.put("--settingsfile", settingsFile);
        params.put("--mapmethod", mapMethod);
        params.put("--license", license);

        Set<String> booleanParams = Set.of("--sourcetrusted","--targettrusted","--useworktables");
        Set<String> sensitiveKeys = Set.of("--sourcepassword", "--targetpassword", "--license");

        for (Map.Entry<String, Property<?>> entry : params.entrySet()) {
            String key = entry.getKey();
            Property<?> prop = entry.getValue();

            // Rendre la valeur en String
            String value = runContext.render((Property<String>) prop).as(String.class).orElse(null);

            if (value == null || value.isEmpty()) {
                continue; // Ignore paramètres non renseignés
            }

            // Si c'est un paramètre booléen et la valeur est "true", on ajoute juste la clé (flag)
            if (booleanParams.contains(key)) {
                if (Boolean.parseBoolean(value)) {
                    command.add(key);
                    commandLog.add(key);
                }
                // Si false, on n'ajoute rien (pas de flag)
            } else {
                // Pour les autres paramètres, on ajoute clé + valeur
                command.add(key);
                command.add(value);
                commandLog.add(key);
                commandLog.add(sensitiveKeys.contains(key) ? "*******" : value);
            }
        }

        logger.info("Command to execute: {}", String.join(" ", commandLog));


        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        logger.info("Starting FastTransfer process...");
        Process process = pb.start();

        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            output = reader.lines().collect(Collectors.joining("\n"));
        }

        int exitCode = process.waitFor();

        logger.info("FastTransfer output:\n{}", output);
        logger.info("Process exited with code {}", exitCode);

        if (exitCode != 0) {
            throw new RuntimeException("FastTransfer executable failed with exit code " + exitCode + "\nOutput:\n" + output);
        }

        return Output.builder()
            .logs(output)
            .exitCode(exitCode)
            .build();
    }



    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Console output",
            description = "Raw output printed by the FastTransfer binary (stdout and stderr merged)"
        )
        private final String logs;

        @Schema(
            title = "Exit code",
            description = "Exit code of the FastTransfer process"
        )
        private final Integer exitCode;
    }



}
