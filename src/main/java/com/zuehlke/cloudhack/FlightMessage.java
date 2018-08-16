package com.zuehlke.cloudhack;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class FlightMessage {

    private String message;
    @SerializedName("message-type") private String messageType;
    @SerializedName("flight-number") private String flightNumber;
    @SerializedName("timestamp") private String eventTime;
    private String processingTime;

    public static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        fields.add(new TableFieldSchema().setName("messageType").setType("STRING"));
        fields.add(new TableFieldSchema().setName("eventTime").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("processingTime").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("flightNumber").setType("STRING"));
        return new TableSchema().setFields(fields);
    }

    public static FlightMessage fromJSON(String msg) {
        FlightMessage flightMessage = new Gson().fromJson(msg, FlightMessage.class);
        flightMessage.setProcessingTime(Instant.now().toString());
        return flightMessage;
    }

    public TableRow getTableRow() {
        return new TableRow()
                .set("message", message)
                .set("messageType", messageType)
                .set("eventTime", eventTime)
                .set("processingTime", processingTime)
                .set("flightNumber", flightNumber);
    }

    public void setProcessingTime(String processingTime) {
        this.processingTime = processingTime;
    }
}
