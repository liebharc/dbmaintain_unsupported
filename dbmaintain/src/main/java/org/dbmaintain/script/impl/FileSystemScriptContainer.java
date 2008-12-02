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
package org.dbmaintain.script.impl;

import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


/**
 * Script container that looks for scripts in a file system directory and its subdirectories. The 
 * scripts directory can optionally contain config file {@link #LOCATION_PROPERTIES_FILENAME}, that
 * defines all properties that are applicable to the script organization.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class FileSystemScriptContainer extends BaseScriptContainer {

    /**
     * The root directory where scripts are located
     */
    protected File scriptLocation;

    /**
     * Constructor for FileScriptContainer.
     *
     * @param scriptLocation
     * @param defaultScriptFileExtensions
     * @param defaultTargetDatabasePrefix
     * @param defaultQualifierPefix 
     * @param defaultPatchQualifiers 
     * @param defaultPostProcessingScriptDirName
     *
     * @param defaultFixSuffix
     * @param defaultScriptEncoding
     */
    public FileSystemScriptContainer(File scriptLocation, Set<String> defaultScriptFileExtensions, String defaultTargetDatabasePrefix, 
            String defaultQualifierPefix, Set<String> defaultPatchQualifiers, String defaultPostProcessingScriptDirName, 
            String defaultScriptEncoding) {
        this.scriptLocation = scriptLocation;
        initConfiguration(defaultScriptFileExtensions, defaultTargetDatabasePrefix, defaultQualifierPefix, defaultPatchQualifiers,
                defaultPostProcessingScriptDirName, defaultScriptEncoding);
        initScripts();
    }


    protected void initScripts() {
        scripts = new ArrayList<Script>();
        getScriptsAt(scripts, scriptLocation.getAbsolutePath(), "");
    }


    /**
     * @param defaultScriptFileExtensions
     * @param defaultTargetDatabasePrefix
     * @param defaultPostProcessingScriptDirName
     *
     * @param defaultScriptEncoding
     */
    private void initConfiguration(Set<String> defaultScriptFileExtensions, String defaultTargetDatabasePrefix, 
            String defaultQualifierPrefix, Set<String> defaultPatchQualifiers, String defaultPostProcessingScriptDirName,
            String defaultScriptEncoding) {
        Properties properties = getLocationCustomProperties();
        if (properties != null) {
            initConfigurationFromProperties(properties);
        } else {
            this.scriptFileExtensions = defaultScriptFileExtensions;
            this.targetDatabasePrefix = defaultTargetDatabasePrefix;
            this.qualifierPrefix = defaultQualifierPrefix;
            this.patchQualifiers = defaultPatchQualifiers;
            this.postProcessingScriptDirName = defaultPostProcessingScriptDirName;
            this.scriptEncoding = defaultScriptEncoding;
        }
        assertValidScriptExtensions();
        assertValidScriptLocation();
    }


    protected Properties getLocationCustomProperties() {
        File customPropertiesFileLocation = new File(scriptLocation + "/" + LOCATION_PROPERTIES_FILENAME);
        if (!customPropertiesFileLocation.exists()) {
            return null;
        }
        InputStream propertiesInputStream = null;
        try {
            Properties properties = new Properties();
            propertiesInputStream = new FileInputStream(customPropertiesFileLocation);
            properties.load(propertiesInputStream);
            return properties;
        } catch (IOException e) {
            throw new DbMaintainException("Error while reading configuration file " + customPropertiesFileLocation, e);
        } finally {
            IOUtils.closeQuietly(propertiesInputStream);
        }
    }


    /**
     * Adds all scripts available in the given directory or one of its subdirectories to the
     * given List of files
     *
     * @param scriptList
     * @param scriptRoot
     * @param relativeLocation
     */
    protected void getScriptsAt(List<Script> scriptList, String scriptRoot, String relativeLocation) {
        File currentLocation = new File(scriptRoot + "/" + relativeLocation);
        if (currentLocation.isFile() && isScriptFile(currentLocation)) {
            Script script = createScript(currentLocation, relativeLocation);
            scriptList.add(script);
            return;
        }
        // recursively scan sub folders for script files
        if (currentLocation.isDirectory()) {
            for (File subLocation : currentLocation.listFiles()) {
                getScriptsAt(scriptList, scriptRoot,
                        "".equals(relativeLocation) ? subLocation.getName() : relativeLocation + '/' + subLocation.getName());
            }
        }
    }


    /**
     * Indicates if the given file is a database update script file
     *
     * @param file The file, not null
     * @return True if the given file is a database update script file
     */
    protected boolean isScriptFile(File file) {
        String name = file.getName();
        for (String scriptFileExtension : scriptFileExtensions) {
            if (name.endsWith(scriptFileExtension)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a script object for the given script file
     *
     * @param scriptFile             The script file, not null
     * @param relativeScriptFileName The name of the script file relative to the root scripts dir, not null
     * @return The script, not null
     */
    protected Script createScript(File scriptFile, String relativeScriptFileName) {
        ScriptContentHandle scriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(FileUtils.getUrl(scriptFile), scriptEncoding);
        return new Script(relativeScriptFileName, scriptFile.lastModified(), scriptContentHandle, targetDatabasePrefix, qualifierPrefix, 
                patchQualifiers, postProcessingScriptDirName);
    }


    protected void assertValidScriptLocation() {
        if (!scriptLocation.exists()) {
            throw new DbMaintainException("Script file location " + scriptLocation + " doesn't exist");
        }
    }


    protected void assertValidScriptExtensions() {
        // check whether an extension is configured
        if (scriptFileExtensions.isEmpty()) {
            throw new DbMaintainException("No script file extensions specified!");
        }
        // Verify the correctness of the script extensions
        for (String extension : scriptFileExtensions) {
            if (extension.startsWith(".")) {
                throw new DbMaintainException("Script file extension " + extension + " should not start with a '.'");
            }
        }
    }

}