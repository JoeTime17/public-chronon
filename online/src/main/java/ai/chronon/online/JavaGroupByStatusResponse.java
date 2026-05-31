package ai.chronon.online;

import ai.chronon.online.fetcher.Fetcher;

public class JavaGroupByStatusResponse {
    public String groupByName;
    public String batchEndDate;

    public JavaGroupByStatusResponse(String groupByName, String batchEndDate) {
        this.groupByName = groupByName;
        this.batchEndDate = batchEndDate;
    }

    public JavaGroupByStatusResponse(Fetcher.GroupByStatusResponse scalaResponse) {
        this.groupByName = scalaResponse.groupByName();
        this.batchEndDate = scalaResponse.batchEndDate();
    }

    public Fetcher.GroupByStatusResponse toScala() {
        return new Fetcher.GroupByStatusResponse(groupByName, batchEndDate);
    }
}
