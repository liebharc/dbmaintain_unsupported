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
import org.dbmaintain.structure.model.DbItemType;
import org.dbmaintain.util.DbMaintainException;

public class StructureUtils {
	
	/**
	 * Raises an exception if at least one of the DB items in itemsToPreserve does not exist in any of the databases.
	 * 
	 *  In case of success the method returns without exception.
	 *  
	 * @param databases databases which should contain the itemsToPreserve
	 * @param itemsToPreserve items which must exist otherwise a runtime exception is raised
	 */
	public static void assertItemsToPreserveExist(Databases databases, Set<DbItemIdentifier> itemsToPreserve) {        
        Set<DbItemIdentifier> unknownItems = filterDbMaintainIdentifiers(itemsToPreserve);
        for (Database database : databases.getDatabases()) {
            if (database == null) {
                // the database is disabled, skip
                continue;
            }
            
            for (String schemaName : database.getSchemaNames()) {
            	unknownItems = filterSchema(unknownItems, database, schemaName);
            	for (DbItemType type : DbItemType.values()) {
            		if (type == SCHEMA) {
            			continue;
            		}
            		unknownItems = removeDbItemOfGivenTypeInSchema(type, unknownItems, database, schemaName);
            	}
            }

        }
        if (unknownItems.size() > 0) {
        	Set<DbItemIdentifier> unknownSchemas = extractSchemas(unknownItems);
        	unknownItems.removeAll(unknownSchemas);
        	Set<String> unknownSchemaNames = mapSchemaItemsToSchemaNames(unknownSchemas);
        	String error = buildErrorMessage(unknownItems, unknownSchemaNames);
        	throw new DbMaintainException(error);
        }
    }

	private static Set<DbItemIdentifier> filterDbMaintainIdentifiers(
			Set<DbItemIdentifier> items) {
		Set<DbItemIdentifier> filtered = new HashSet<DbItemIdentifier>();
        for (DbItemIdentifier item : items) {
        	if (!item.isDbMaintainIdentifier())
        		filtered.add(item);
        }
		return filtered;
	}
	
	private static Set<DbItemIdentifier> filterSchema(Set<DbItemIdentifier> items,
			Database database, String schemaName) {
		Set<DbItemIdentifier> filtered = new HashSet<DbItemIdentifier>();
		filtered.addAll(items);
		DbItemIdentifier schema = getItemIdentifier(SCHEMA, schemaName, null, database);
		filtered.remove(schema);
		return filtered;
	}	
	
	private static Set<DbItemIdentifier> removeDbItemOfGivenTypeInSchema(DbItemType type, Set<DbItemIdentifier> items,
			Database database, String schemaName) {
		Set<DbItemIdentifier> filtered = new HashSet<DbItemIdentifier>();
		filtered.addAll(items);
		if (!database.supports(type)) {
			return filtered;
		}
		Set<DbItemIdentifier> itemNames = toDbItemIdentifiers(type, database, schemaName, database.getDbItemsOfType(type, schemaName));
		filtered.removeAll(itemNames);
		return filtered;
	}	
	
	private static Set<DbItemIdentifier> extractSchemas(
			Set<DbItemIdentifier> items) {
		Set<DbItemIdentifier> schemas = new HashSet<DbItemIdentifier>();
		for (DbItemIdentifier item : items) {
			if(item.getType() == SCHEMA)
				schemas.add(item);
		}
		return schemas;
	}
	
	private static Set<String> mapSchemaItemsToSchemaNames(
			Set<DbItemIdentifier> schemas) {
		Set<String> schemaNames = new HashSet<String>();
		for (DbItemIdentifier schema : schemas) {
			schemaNames.add(schema.getSchemaName());
		}
		return schemaNames;
	}	
	
	private static String buildErrorMessage(Set<DbItemIdentifier> unknownItems,
			Set<String> unknownSchemaNames) {
		String error = "";
		if (unknownSchemaNames.size() > 0)
			error += "Schemas to preserve do not exist: " + StringUtils.join(unknownSchemaNames, ", ") + "\n";
		if (unknownItems.size() > 0)
			error += "Tables to preserve do not exist: " + StringUtils.join(unknownItems, ", ") + "\n";
		error += "DbMaintain cannot determine which schema's need to be preserved. To assure nothing is deleted by mistake, nothing will be deleted.";
		return error;
	}	
	
    private static Set<DbItemIdentifier> toDbItemIdentifiers(DbItemType type, Database database, String schemaName, Set<String> itemNames) {
        Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
        for (String itemName : itemNames) {
            result.add(getItemIdentifier(type, schemaName, itemName, database));
        }
        return result;
    }	
}
