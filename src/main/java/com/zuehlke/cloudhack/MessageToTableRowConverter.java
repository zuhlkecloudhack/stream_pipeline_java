package com.zuehlke.cloudhack;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

public class MessageToTableRowConverter extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        FlightMessage fm = FlightMessage.fromJSON(c.element());
        c.output(fm.getTableRow());
    }
}
