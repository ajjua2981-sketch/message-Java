package com.dams.messageparsing.db;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Oracle data-access layer.
 * Uses Spring's JdbcTemplate so connection pooling (HikariCP) is managed
 * automatically from spring.datasource.* properties.
 */
@Component
public class OracleHandler {

    private static final Logger logger = LoggerFactory.getLogger(OracleHandler.class);

    // Only allow plain SQL identifiers to prevent SQL injection via env vars.
    private static final Pattern VALID_IDENTIFIER =
        Pattern.compile("^[A-Za-z_][A-Za-z0-9_$#]{0,127}$");

    private final JdbcTemplate jdbc;

    @Value("${app.oracle.table}")
    private String table;

    @Value("${app.oracle.payload-column:DAMSRESPONSETEXT}")
    private String payloadColumn;

    public OracleHandler(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    public void validate() {
        if (!VALID_IDENTIFIER.matcher(table).matches()) {
            throw new IllegalStateException(
                "app.oracle.table contains an invalid SQL identifier: " + table);
        }
        if (!VALID_IDENTIFIER.matcher(payloadColumn).matches()) {
            throw new IllegalStateException(
                "app.oracle.payload-column contains an invalid SQL identifier: " + payloadColumn);
        }
        logger.info("Oracle handler ready — table: {}  payload column: {}", table, payloadColumn);
    }

    /**
     * Returns all rows matching the reference ID, ordered by ROWID (insertion order).
     */
    public List<Map<String, Object>> fetchByReferenceId(String referenceId) {
        String sql = String.format(
            "SELECT ROWID, t.* FROM %s t WHERE REFERENCE_ID = ? ORDER BY ROWID ASC",
            table);
        return jdbc.queryForList(sql, referenceId);
    }

    /**
     * Updates the payload column of the row identified by the given Oracle ROWID.
     */
    public void updatePayload(String rowid, String referenceId, String payload) {
        String sql = String.format(
            "UPDATE %s SET %s = ? WHERE ROWID = ?",
            table, payloadColumn);
        jdbc.update(sql, payload, rowid);
        logger.info("Updated {} for REFERENCE_ID={}", payloadColumn, referenceId);
    }
}
