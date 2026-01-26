package egm.io.nifi.processors.ngsild.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class TestUtils {
    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static void printTableContent(Connection connection, String schema, String tableName) throws SQLException {
        String query = String.format("SELECT * FROM %s.%s", schema, tableName);

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            logger.info("--- Table Content: " + schema + "." + tableName + " ---");
            while (resultSet.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    row.append(metaData.getColumnName(i)).append(": ").append(resultSet.getString(i)).append(" | ");
                }
                logger.info(row.toString());
            }
            logger.info("------------------------------------------");
        }
    }

}