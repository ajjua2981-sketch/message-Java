package com.dams.messageparsing.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class XmlToJsonParser {

    private static final Logger logger = LoggerFactory.getLogger(XmlToJsonParser.class);

    private final XmlMapper xmlMapper = new XmlMapper();
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public record ParseResult(Map<String, Object> data, String json) {}

    public ParseResult parse(String xmlString) throws Exception {
        Map<String, Object> data = xmlMapper.readValue(
            xmlString, new TypeReference<Map<String, Object>>() {});
        logger.debug("XML parsed successfully");
        return new ParseResult(data, jsonMapper.writeValueAsString(data));
    }
}
