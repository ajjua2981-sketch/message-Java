package com.dams.messageparsing.processor;

import com.dams.messageparsing.db.OracleHandler;
import com.dams.messageparsing.parser.ParseResult;
import com.dams.messageparsing.parser.XmlToJsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Orchestrates the full pipeline for a single message:
 * XML → JSON → fetch by reference ID → update payload column.
 */
@Component
public class MessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    @Value("${app.reference-id-path}")
    private String referenceIdPath;

    private final XmlToJsonParser parser;
    private final OracleHandler oracleHandler;

    public MessageProcessor(XmlToJsonParser parser, OracleHandler oracleHandler) {
        this.parser = parser;
        this.oracleHandler = oracleHandler;
    }

    /**
     * Processes one XML message end-to-end.
     *
     * @throws PermanentMessageException for malformed XML or a missing reference-ID path
     * @throws Exception                 for transient errors (DB down, etc.)
     */
    public void processMessage(String xmlString) throws Exception {

        // 1. Parse XML → JSON
        ParseResult result;
        try {
            result = parser.parse(xmlString);
        } catch (Exception e) {
            throw new PermanentMessageException("XML parse failed: " + e.getMessage(), e);
        }

        // 2. Extract reference ID via configured dot-notation path
        String referenceId;
        try {
            referenceId = extractReferenceId(result.data());
        } catch (Exception e) {
            throw new PermanentMessageException("Missing field in message: " + e.getMessage(), e);
        }

        logger.info("Processing REFERENCE_ID={}", referenceId);

        // 3. Fetch existing Oracle records
        List<Map<String, Object>> records = oracleHandler.fetchByReferenceId(referenceId);
        int count = records.size();

        if (count == 0) {
            // No matching record — skip update intentionally; Kafka offset still committed
            logger.warn("No record found for REFERENCE_ID={} — skipping update", referenceId);
            return;
        }

        // 4. Update payload column of the first record (ROWID is fastest Oracle row locator)
        Map<String, Object> firstRow = records.get(0);
        String rowid = String.valueOf(firstRow.get("ROWID"));
        oracleHandler.updatePayload(rowid, referenceId, result.json());

        if (count > 1) {
            logger.warn("Multiple records ({}) found for REFERENCE_ID={} — updated the first one",
                count, referenceId);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Walks the dot-notation path (e.g. "ns2:Envelope.ns2:body.PAReferenceId")
     * through the parsed XML map to locate the reference ID value.
     */
    @SuppressWarnings("unchecked")
    private String extractReferenceId(Map<String, Object> data) {
        String[] segments = referenceIdPath.split("\\.");
        Object current = data;

        for (String segment : segments) {
            if (!(current instanceof Map)) {
                throw new IllegalArgumentException(
                    "Expected map at segment '" + segment + "' but got: " +
                    (current == null ? "null" : current.getClass().getSimpleName()));
            }
            Map<String, Object> map = (Map<String, Object>) current;
            if (!map.containsKey(segment)) {
                throw new IllegalArgumentException(
                    "Could not resolve path '" + referenceIdPath +
                    "' — segment '" + segment + "' not found. Available keys: " + map.keySet());
            }
            current = map.get(segment);
        }

        return String.valueOf(current);
    }
}
