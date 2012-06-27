package org.dbmaintain.structure;

import static org.dbmaintain.structure.model.DbItemIdentifier.getItemIdentifier;
import static org.dbmaintain.structure.model.DbItemType.SCHEMA;
import static org.dbmaintain.structure.model.DbItemType.TABLE;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.structure.model.DbItemIdentifier;
import org.dbmaintain.util.DbMaintainException;

public class StructureUtils {
	
	public static void assertItemsToPreserveExist(Databases databases, Set<DbItemIdentifier> itemsToPreserve) {        
        Set<DbItemIdentifier> unknownItems = new HashSet<DbItemIdentifier>();
        for (DbItemIdentifier item : itemsToPreserve) {
        	if (!item.isDbMaintainIdentifier())
        		unknownItems.add(item);
        }
        for (Database database : databases.getDatabases()) {
            if (database == null) {
                // the database is disabled, skip
                continue;
            }
            
            for (String schemaName : database.getSchemaNames()) {
            	DbItemIdentifier schema = getItemIdentifier(SCHEMA, schemaName, null, database);
            	unknownItems.remove(schema);
            	Set<DbItemIdentifier> tableNames  = toDbItemIdentifiers(database, schemaName, database.getTableNames(schemaName));
            	unknownItems.removeAll(tableNames);
            }

        }
        if (unknownItems.size() > 0) {
        	Set<DbItemIdentifier> unknownSchemas = new HashSet<DbItemIdentifier>();
        	for (DbItemIdentifier item : unknownItems) {
        		if(item.getType() == SCHEMA)
        			unknownSchemas.add(item);
        	}
        	unknownItems.removeAll(unknownSchemas);
        	Set<String> unknownSchemaNames = new HashSet<String>();
        	for (DbItemIdentifier schema : unknownSchemas) {
        		unknownSchemaNames.add(schema.getSchemaName());
        	}
        	String error = "";
        	if (unknownSchemas.size() > 0)
        		error += "Schemas to preserve do not exist: " + StringUtils.join(unknownSchemaNames, ", ") + "\n";
        	if (unknownItems.size() > 0)
        		error += "Tables to preserve do not exist: " + StringUtils.join(unknownItems, ", ") + "\n";
        	error += "DbMaintain cannot determine which schema's need to be preserved. To assure nothing is deleted by mistake, nothing will be deleted.";
        	throw new DbMaintainException(error);
        }
    }
	
    public static Set<DbItemIdentifier> toDbItemIdentifiers(Database database, String schemaName, Set<String> itemNames) {
        Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
        for (String itemName : itemNames) {
            result.add(getItemIdentifier(TABLE, schemaName, itemName, database));
        }
        return result;
    }	
}
