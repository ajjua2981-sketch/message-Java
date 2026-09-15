package com.dams.messageparsing.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Converts an XML string to a nested Map and then to a compact JSON string.
 *
 * <p>Jackson's XmlMapper preserves XML namespace prefixes in Map keys,
 * so an element {@code <ns2:Envelope>} becomes the key {@code "ns2:Envelope"}.
 * This means the reference-ID path configured in app.reference-id-path uses
 * the same dot-notation as the Python version.</p>
 */
@Component
public class XmlToJsonParser {

    private static final Logger logger = LoggerFactory.getLogger(XmlToJsonParser.class);

    private final XmlMapper xmlMapper = new XmlMapper();
    private final ObjectMapper jsonMapper = new ObjectMapper();

    /**
     * Parses the XML, serialises it to compact JSON, and returns both.
     *
     * @throws Exception if the XML is malformed or cannot be serialised
     */
    public ParseResult parse(String xmlString) throws Exception {
        Map<String, Object> data = xmlMapper.readValue(
            xmlString, new TypeReference<Map<String, Object>>() {});
        logger.debug("XML parsed successfully");

        String json = jsonMapper.writeValueAsString(data);
        return new ParseResult(data, json);
    }
}
