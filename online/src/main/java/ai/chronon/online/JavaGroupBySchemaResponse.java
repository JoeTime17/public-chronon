package ai.chronon.online;

import ai.chronon.online.fetcher.Fetcher;

public class JavaGroupBySchemaResponse {
    public String groupByName;
    public String keySchema;
    public String valueSchema;
    public String inputSchema;
    public String selectedSchema;

    public JavaGroupBySchemaResponse(String groupByName,
                                     String keySchema,
                                     String valueSchema,
                                     String inputSchema,
                                     String selectedSchema) {
        this.groupByName = groupByName;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.inputSchema = inputSchema;
        this.selectedSchema = selectedSchema;
    }

    public JavaGroupBySchemaResponse(Fetcher.GroupBySchemaResponse scalaResponse) {
        this.groupByName = scalaResponse.groupByName();
        this.keySchema = scalaResponse.keySchema();
        this.valueSchema = scalaResponse.valueSchema();
        this.inputSchema = scalaResponse.inputSchema();
        this.selectedSchema = scalaResponse.selectedSchema();
    }

    public Fetcher.GroupBySchemaResponse toScala() {
        return new Fetcher.GroupBySchemaResponse(
                groupByName,
                keySchema,
                valueSchema,
                inputSchema,
                selectedSchema);
    }
}
