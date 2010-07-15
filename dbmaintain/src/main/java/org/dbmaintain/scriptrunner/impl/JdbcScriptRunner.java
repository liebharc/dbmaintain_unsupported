/*
 * Copyright 2006-2007,  Unitils.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbmaintain.scriptrunner.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.script.Script;
import org.dbmaintain.scriptparser.ScriptParser;
import org.dbmaintain.scriptparser.ScriptParserFactory;
import org.dbmaintain.scriptrunner.ScriptRunner;
import org.dbmaintain.util.DbMaintainException;

import java.io.Reader;
import java.util.Map;

import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * Default implementation of a script runner that uses JDBC to execute the script.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class JdbcScriptRunner implements ScriptRunner {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(JdbcScriptRunner.class);

    protected DbSupports dbSupports;
    protected SQLHandler sqlHandler;
    protected Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap;


    public JdbcScriptRunner(Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap, DbSupports dbSupports, SQLHandler sqlHandler) {
        this.databaseDialectScriptParserFactoryMap = databaseDialectScriptParserFactoryMap;
        this.dbSupports = dbSupports;
        this.sqlHandler = sqlHandler;
    }


    /**
     * Executes the given script.
     * <p/>
     * All statements should be separated with a semicolon (;). The last statement will be
     * added even if it does not end with a semicolon.
     *
     * @param script The script, not null
     */
    public void execute(Script script) {
        Reader scriptContentReader = null;
        try {
            // Define the target database on which to execute the script
            DbSupport targetDbSupport = getTargetDatabaseDbSupport(script);
            if (targetDbSupport == null) {
                logger.info("Script " + script.getFileName() + " has target database " + script.getTargetDatabaseName() + ". This database is disabled, so the script is not executed.");
                return;
            }

            // get content stream
            scriptContentReader = script.getScriptContentHandle().openScriptContentReader();
            // create a script parser for the target database in question 
            ScriptParser scriptParser = databaseDialectScriptParserFactoryMap.get(targetDbSupport.getSupportedDatabaseDialect()).createScriptParser(scriptContentReader);
            // parse and execute the statements
            String statement;
            while ((statement = scriptParser.getNextStatement()) != null) {
                sqlHandler.executeUpdateAndCommit(statement, targetDbSupport.getDataSource());
            }
        } finally {
            closeQuietly(scriptContentReader);
        }
    }

    public void initialize() {
        // nothing to initialize
    }

    public void close() {
        // nothing to close
    }

    /**
     * @param script The script, not null
     * @return The db support to use for the script, not null
     */
    protected DbSupport getTargetDatabaseDbSupport(Script script) {
        String databaseName = script.getTargetDatabaseName();
        if (databaseName == null) {
            return dbSupports.getDefaultDbSupport();
        }
        if (!dbSupports.isConfiguredDatabase(databaseName)) {
            throw new DbMaintainException("Error executing script " + script.getFileName() + ". No database initialized with the name " + script.getTargetDatabaseName());
        }
        return dbSupports.getDbSupport(databaseName);
    }

}