/**
 * Copyright 2010-2014 Axel Fontaine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.dbsupport.phoenix;

import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.Table;
import org.flywaydb.core.internal.util.StringUtils;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Phoenix implementation of Schema.
 */
public class PhoenixSchema extends Schema<PhoenixDbSupport> {
    private static final Log LOG = LogFactory.getLog(PhoenixSchema.class);

    /**
     * Creates a new Phoenix schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param dbSupport    The database-specific support.
     * @param name         The name of the schema.
     */
    public PhoenixSchema(JdbcTemplate jdbcTemplate, PhoenixDbSupport dbSupport, String name) {
        super(jdbcTemplate, dbSupport, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        if (name == null) {
            return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE table_schem IS NULL") > 0;
        }
        else {
            return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE table_schem=?", name) > 0;
        }
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return allTables().length == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        LOG.info("Phoenix does not support creating schemas. Schema not dropped: " + name);
    }

    @Override
    protected void doDrop() throws SQLException {
        LOG.info("Phoenix does not support dropping schemas. Schema not dropped: " + name);
    }

    @Override
    protected void doClean() throws SQLException {
        for (Table table : allTables()) {
            table.drop();
        }

        List<String> sequenceNames = listObjectsOfType("sequence");
        for (String statement : generateDropStatements("SEQUENCE", sequenceNames, "")) {
            jdbcTemplate.execute(statement);
        }

        List<String> indexNames = listObjectsOfType("index");
        for (String statement : generateDropStatements("INDEX", indexNames, "")) {
            jdbcTemplate.execute(statement);
        }

        List<String> viewNames = listObjectsOfType("view");
        for (String statement : generateDropStatements("INDEX", viewNames, "")) {
            jdbcTemplate.execute(statement);
        }
    }

    /**
     * Generate the statements for dropping all the objects of this type in this schema.
     *
     * @param objectType          The type of object to drop (Sequence, constant, ...)
     * @param objectNames         The names of the objects to drop.
     * @param dropStatementSuffix Suffix to append to the statement for dropping the objects.
     * @return The list of statements.
     */
    private List<String> generateDropStatements(String objectType, List<String> objectNames, String dropStatementSuffix) {
        List<String> statements = new ArrayList<String>();
        for (String objectName : objectNames) {
            String dropStatement =
                    "DROP " + objectType + dbSupport.quote(name, objectName) + " " + dropStatementSuffix;

            statements.add(dropStatement);
        }
        return statements;
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        List<String> tableNames = listObjectsOfType("table");

        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new PhoenixTable(jdbcTemplate, dbSupport, this, tableNames.get(i));
        }
        return tables;
    }

    /**
     * List the names of the objects of this type in this schema.
     *
     * @return The names of the objects.
     * @throws java.sql.SQLException when the object names could not be listed.
     */

    protected List<String> listObjectsOfType(String type) throws SQLException {
        String tableType = "";

        if (type.equalsIgnoreCase("sequence")) {
            if(name == null) {
                String query = "SELECT SEQUENCE_NAME FROM SYSTEM.\"SEQUENCE\" WHERE SEQUENCE_SCHEMA IS NULL";
                return jdbcTemplate.queryForStringList(query);
            }
            else {
                String query = "SELECT SEQUENCE_NAME FROM SYSTEM.\"SEQUENCE\" WHERE SEQUENCE_SCHEMA = ?";
                return jdbcTemplate.queryForStringList(query, name);
            }
        }

        if (type.equalsIgnoreCase("table")) {
            tableType = "u";
        }
        else if (type.equalsIgnoreCase("index")) {
            tableType = "i";
        }
        else if (type.equalsIgnoreCase("view")) {
            tableType = "v";
        }

        String queryStart = "SELECT TABLE_NAME FROM SYSTEM.CATALOG WHERE TABLE_SCHEM";
        String queryEnd = " AND TABLE_TYPE = '" + tableType + "'";
        String queryMid = "";

        if(name == null) {
            queryMid += " IS NULL";
            return jdbcTemplate.queryForStringList(queryStart + queryMid + queryEnd);
        }
        else {
            queryMid += " = ?";
            return jdbcTemplate.queryForStringList(queryStart + queryMid + queryEnd, name);
        }
    }

    @Override
    public Table getTable(String tableName) {
        return new PhoenixTable(jdbcTemplate, dbSupport, this, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Schema schema = (Schema) o;
        if(name == null) {
            return name == schema.getName();
        }
        else {
            return name.equals(schema.getName());
        }
    }
}
