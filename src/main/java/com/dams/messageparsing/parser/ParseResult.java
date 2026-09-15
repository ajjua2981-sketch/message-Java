package com.dams.messageparsing.parser;

import java.util.Map;

/**
 * Holds both the parsed XML as a nested Map and the compact JSON string
 * that will be stored in Oracle.
 */
public record ParseResult(Map<String, Object> data, String json) {}
