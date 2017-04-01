/* 
 * The Fascinator - Indexer
 * Copyright (C) 2009-2011 University of Southern Queensland
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.fascinator.indexer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import javax.naming.directory.SearchResult;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.python.core.Py;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.indexer.IndexerException;
import com.googlecode.fascinator.api.indexer.SearchRequest;
import com.googlecode.fascinator.api.indexer.rule.RuleException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.PythonUtils;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;
import com.googlecode.fascinator.common.solr.SolrResult;
import com.googlecode.fascinator.common.solr.SolrDoc;

/**
 * <p>
 * This plugin provides indexing services for DigitalObjects and payloads in The
 * Fascinator through <a href="http://lucene.apache.org/solr/">Apache Solr</a>.
 * </p>
 * 
 * <h3>Configuration</h3>
 * <p>
 * Standard configuration table:
 * </p>
 * <table border="1">
 * <tr>
 * <th>Option</th>
 * <th>Description</th>
 * <th>Required</th>
 * <th>Default</th>
 * </tr>
 * 
 * <tr>
 * <td>uri</td>
 * <td>The URI of the Solr service</td>
 * <td><b>Yes</b></td>
 * <td>http://localhost:9997/solr/fascinator</td>
 * </tr>
 * 
 * <tr>
 * <td>coreName</td>
 * <td>The name of the indexer</td>
 * <td><b>Yes</b></td>
 * <td>fascinator</td>
 * </tr>
 * 
 * <tr>
 * <td>embedded</td>
 * <td>If false, Solr is started as a separate web application. If true, Solr
 * will start in embedded mode which is non-HTTP accessible. <b>Note:</b> that
 * the current version of Solr (1.4) has bugs when running as embedded, version
 * 1.3 is more stable, so it's recommended to keep this option set to
 * false.</td>
 * <td><b>Yes</b></td>
 * <td>false</td>
 * </tr>
 * 
 * </table>
 * 
 * <h3>Examples</h3>
 * <ol>
 * <li>Using Internal authentication plugin in The Fascinator
 * 
 * <pre>
 *         "solr": {
 *             "uri": "http://localhost:9997/solr/fascinator",
 *             "coreName": "fascinator",
 *             "autocommit": false,
 *             "embedded": true
 *         }
 * </pre>
 * 
 * </li>
 * </ol>
 * 
 * <h3>Rule file</h3>
 * <p>
 * The Solr Indexer takes a rules file to assist it in indexing various content.
 * This allows you to set up indexing rules for individual harvests. For
 * example, harvests that are transformed by the Aperture plugin need indexing
 * on RDF whereas OAI-PMH Harvests probably don't need transforming and the XML
 * DC is indexed.
 * </p>
 * 
 * <h3>Wiki Link</h3>
 * <p>
 * None
 * </p>
 * 
 * @author Greg Pendlebury
 */

public class SolrIndexHandler{

	/** A fake OID for Anotar - Used in caching/execution */
	private static String ANOTAR_RULES_OID = "FakeAnotarRulesOid1234";

	/** The name of the core class inside rules files */
	private static String SCRIPT_CLASS_NAME = "IndexData";

	/** The name of the activation method required on instantiated classes */
	private static String SCRIPT_ACTIVATE_METHOD = "__activate__";

	/** Default payload for object metadata */
	private static String DEFAULT_METADATA_PAYLOAD = "TF-OBJ-META";

	/** Actual payload for object metadata */
	private String propertiesId;

	/** Logging */
	private Logger log = LoggerFactory.getLogger(SolrIndexHandler.class);

	/** Configuration */
	private JsonSimpleConfig config;

	/** Storage API */
	private Storage storage;

	/** Main Solr core */
	private SolrServer solr;

	/** Anotar core */
	private SolrServer anotar;

	/** Auto-commit flag for anotar core */
	private boolean anotarAutoCommit;

	/** Usernames for Solr cores */
	private Map<String, String> usernameMap;

	/** Passwords for Solr cores */
	private Map<String, String> passwordMap;

	/** Flag if init() has been run before */
	private boolean loaded;

	/** Keep track of custom parameters */
	private Map<String, String> customParams;

	/** Utility function for use inside python scripts */
	private PythonUtils pyUtils;

	/** Cache of instantiated python scripts */
	private Map<String, PyObject> scriptCache;

	/** Cache of groovy scripts */
	private Map<String, String> groovyScriptCache;

	/** Cache of instantiated config files */
	private Map<String, JsonSimpleConfig> configCache;

	/** Flag for use of the cache */
	private boolean useCache;

	/** Messaging services */
	private MessagingServices messaging;

	private Map<String, SolrServer> solrServerMap;

	private ScriptEngine engine;


	private PythonUtils getPyUtils() throws IndexerException {
		if (pyUtils == null) {
			try {
				pyUtils = new PythonUtils(config);
			} catch (PluginException ex) {
				throw new IndexerException(ex);
			}
		}
		return pyUtils;
	}

	private MessagingServices getMessaging() {
		if (messaging == null) {
			try {
				messaging = MessagingServices.getInstance();
			} catch (MessagingException ex) {
				log.error("Failed to start connection: {}", ex.getMessage());
			}
		}
		return messaging;
	}

	public SolrIndexHandler() {
		loaded = false;
	}

	

	/**
	 * Private method wrapped by the above two methods to perform the actual
	 * initialization after the JSON config is accessed.
	 * 
	 * @throws IndexerException
	 *             if errors occur during initialization
	 */
	private void init() throws IndexerException {
		if (!loaded) {
			loaded = true;

			String storageType = config.getString(null, "storage", "type");
			try {
				storage = PluginManager.getStorage(storageType);
				storage.init(config.toString());
			} catch (PluginException pe) {
				throw new IndexerException(pe);
			}

			// Credentials
			usernameMap = new HashMap<String, String>();
			passwordMap = new HashMap<String, String>();

			solr = initCore("solr");
			anotar = initCore("anotar");

			// initialise non-hardcoded indexers
			solrServerMap = new HashMap<String, SolrServer>();
			JsonObject indexerConfig = config.getObject("indexer");

			List<String> hardcodedValues = Arrays.asList("type", "properties", "useCache", "buffer", "solr", "anotar");
			for (Object key : indexerConfig.keySet()) {
				if (key instanceof String) {
					String keyString = (String) key;
					if (!hardcodedValues.contains(keyString)) {
						solrServerMap.put(keyString, initCore(keyString));
					}
				}
			}

			anotarAutoCommit = config.getBoolean(true, "indexer", "anotar", "autocommit");
			propertiesId = config.getString(DEFAULT_METADATA_PAYLOAD, "indexer", "propertiesId");

			customParams = new HashMap<String, String>();

			// Caching
			scriptCache = new HashMap<String, PyObject>();
			groovyScriptCache = new HashMap<String, String>();
			configCache = new HashMap<String, JsonSimpleConfig>();
			useCache = config.getBoolean(true, "indexer", "useCache");

		}
		loaded = true;
	}

	/**
	 * Set a value in the custom parameters of the indexer.
	 * 
	 * @param property
	 *            : The index to use
	 * @param value
	 *            : The value to store
	 */
	public void setCustomParam(String property, String value) {
		customParams.put(property, value);
	}

	/**
	 * Initialize a Solr core object.
	 * 
	 * @param coreName
	 *            : The core to initialize
	 * @return SolrServer : The initialized core
	 */
	private SolrServer initCore(String coreName) {
		try {
			String uri = config.getString(null, "indexer", coreName, "uri");
			if (uri == null) {
				log.error("No URI provided for core: '{}'", coreName);
				return null;
			}
			URI solrUri = new URI(uri);
			CommonsHttpSolrServer thisCore = new CommonsHttpSolrServer(solrUri.toURL());
			String username = config.getString(null, "indexer", coreName, "username");
			String password = config.getString(null, "indexer", coreName, "password");
			usernameMap.put(coreName, username);
			passwordMap.put(coreName, password);
			if (!username.equals("") && !password.equals("")) {
				UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
				HttpClient hc = (thisCore).getHttpClient();
				hc.getParams().setAuthenticationPreemptive(true);
				hc.getState().setCredentials(AuthScope.ANY, credentials);
			}
			return thisCore;
		} catch (MalformedURLException mue) {
			log.error(coreName + " : Malformed URL", mue);
		} catch (URISyntaxException urise) {
			log.error(coreName + " : Invalid URI", urise);
		}
		return null;
	}

	/**
	 * Return a reference to this plugins instantiated storage layer
	 * 
	 * @return Storage : the storage API being used
	 */
	public Storage getStorage() {
		return storage;
	}

	

	

	/**
	 * Index a specific payload
	 * 
	 * @param object
	 *            : The payload's object
	 * @param pid
	 *            : The payload
	 * @return 
	 * @throws IndexerException
	 *             if there were errors during indexing
	 */
	public SimpleEntry<String, String> index(DigitalObject object, Payload payload) throws IndexerException {
		String oid = object.getId();
		String pid = payload.getId();

		// Don't proccess annotations through this function
		if (pid.startsWith("anotar.")) {
			annotate(object, payload);
			return new SimpleEntry<String, String>(null, null);
		}
		// log.info("Indexing OID:'{}', PID: '{}'", oid, pid);

		// get the indexer properties or we can't index
		Properties props;
		try {
			props = object.getMetadata();
		} catch (StorageException ex) {
			throw new IndexerException("Failed loading properties : ", ex);
		}

		try {
			// Get the harvest files
			String confOid = props.getProperty("jsonConfigOid");
			String rulesOid = props.getProperty("rulesOid");
			String scriptType = props.getProperty("scriptType");
			String doc;
			// Generate the Solr document
			if ("groovy".equals(scriptType) || StringUtils.isBlank(scriptType)) {
				doc = indexByGroovyScript(object, payload, confOid, rulesOid, props);
			} else {
				doc = indexByPythonScript(object, payload, confOid, rulesOid, props);
			}

			// Did the indexer alter metadata?
			String toClose = props.getProperty("objectRequiresClose");
			if (toClose != null) {
				log.debug("Indexing has altered metadata, closing object.");
				props.remove("objectRequiresClose");
				object.close();
				try {
					props = object.getMetadata();
				} catch (StorageException ex) {
					throw new IndexerException("Failed loading properties : ", ex);
				}
			}

			return new SimpleEntry<String, String>(oid + "/" + pid, doc);
		} catch (Exception e) {
			log.error("Indexing failed!\n-----\n", e);
		}
		return new SimpleEntry<String, String>(null, null);
	}

	/**
	 * Index a payload using the provided data using a groovy script
	 * 
	 * @param object
	 *            : The DigitalObject to index
	 * @param payload
	 *            : The Payload to index
	 * @param in
	 *            : Reader containing the new empty document
	 * @param inConf
	 *            : An InputStream holding the config file
	 * @param rulesOid
	 *            : The oid of the rules file to use
	 * @param props
	 *            : Properties object containing the object's metadata
	 * @return File : Temporary file containing the output to index
	 * @throws IOException
	 *             if there were errors accessing files
	 * @throws RuleException
	 *             if there were errors during indexing
	 */
	private String indexByGroovyScript(DigitalObject object, Payload payload, String confOid, String rulesOid,
			Properties props) throws RuleException {
		try {

			if (engine == null) {
				SimpleBindings bindings = new SimpleBindings();
				ScriptEngineManager manager = new ScriptEngineManager();
				engine = manager.getEngineByName("groovy");

				// the engine is stateful, and multiple queues means that
				// different queues will clash
				// still won't affect different messages coming through the same
				// queue as AMQ will deal with sycnhronization
				// will affect how config is read in the messaging scripts,
				// needs to access it as `configMap[<queue name>]`
				// configMap = new HashMap<String, HashMap<String, Object>>();
				// engine.put("configMap", configMap);
			}

			JsonSimpleConfig jsonConfig = getConfigFile(confOid);

			// Get our data ready
			SimpleBindings bindings = new SimpleBindings();
			Map<String, List<String>> fields = new HashMap<String, List<String>>();
			bindings.put("fields", fields);
			bindings.put("jsonConfig", jsonConfig);
			bindings.put("indexer", this);
			bindings.put("object", object);
			bindings.put("payload", payload);
			bindings.put("params", props);
			bindings.put("log", log);

			// Run the data through our script
			Reader script = getGroovyObject(rulesOid);

			engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
			SolrInputDocument document = (SolrInputDocument) engine.eval(script);
			StringWriter documentString = new StringWriter();
			ClientUtils.writeXML(document, documentString);
			return documentString.toString();
		} catch (Exception e) {
			throw new RuleException(e);
		}
	}

	private Reader getGroovyObject(String oid) throws IOException {
		if (groovyScriptCache.get(oid) != null) {
			return new StringReader(groovyScriptCache.get(oid));
		} else {
			// A standard rules file
			DigitalObject object;
			Payload payload;
			try {
				object = storage.getObject(oid);
				String scriptName = object.getSourceId();
				payload = object.getPayload(scriptName);
				InputStream inStream = payload.open();
				log.debug("First time parsing rules script: '{}'", oid);
				Reader reader = cacheGroovyScript(oid, inStream);
				return reader;
			} catch (StorageException ex) {
				log.error("Rules file could not be retrieved! '{}'", oid, ex);
				return null;
			}
		}
	}

	private Reader cacheGroovyScript(String oid, InputStream inStream) throws IOException {
		StringWriter writer = new StringWriter();
		IOUtils.copy(inStream, writer, Charset.forName("UTF-8").name());
		String scriptString = writer.toString();
		if (useCache) {
			groovyScriptCache.put(oid, scriptString);
		}
		return new StringReader(scriptString);
	}

	/**
	 * Add a new document into the buffer, and check if submission is required
	 * 
	 * @param document
	 *            : The Solr document to add to the buffer.
	 */
	private void addToBuffer(String index, String document) {
		JsonObject message = new JsonObject();
		message.put("event", "index");
		message.put("index", index);
		message.put("document", document);
		sendToIndex(message.toString());
	}

	/**
	 * Send the document to buffer directly
	 * 
	 * @param index
	 * @param fields
	 */
	public void sendIndexToBuffer(String index, Map<String, List<String>> fields) {
		String doc = pyUtils.solrDocument(fields);
		addToBuffer(index, doc);
	}

	/**
	 * To put events to subscriber queue
	 * 
	 * @param oid
	 *            Object id
	 * @param eventType
	 *            type of events happened
	 * @param context
	 *            where the event happened
	 * @param jsonFile
	 *            Configuration file
	 */
	private void sendToIndex(String message) {
		try {
			getMessaging().queueMessage(SolrWrapperQueueConsumer.QUEUE_ID, message);
		} catch (MessagingException ex) {
			log.error("Unable to send message: ", ex);
		}
	}


	/**
	 * Index a specific annotation
	 * 
	 * @param object
	 *            : The annotation's object
	 * @param pid
	 *            : The annotation payload
	 * @throws IndexerException
	 *             if there were errors during indexing
	 */
	void annotate(DigitalObject object, Payload payload) throws IndexerException {
		String pid = payload.getId();
		if (propertiesId.equals(pid)) {
			return;
		}

		try {
			Properties props = new Properties();
			props.setProperty("metaPid", pid);

			String doc = indexByPythonScript(object, payload, null, ANOTAR_RULES_OID, props);
			if (doc != null) {
				doc = "<add>" + doc + "</add>";
				anotar.request(new DirectXmlRequest("/update", doc));
				if (anotarAutoCommit) {
					anotar.commit();
				}
			}
		} catch (Exception e) {
			log.error("Indexing failed!\n-----\n", e);
		}
	}

	public void setPropertiesId(String propertiesId) {
		this.propertiesId = propertiesId;
	}

	public void setConfig(JsonSimpleConfig config) {
		this.config = config;
	}

	public void setAnotar(SolrServer anotar) {
		this.anotar = anotar;
	}

	public void setAnotarAutoCommit(boolean anotarAutoCommit) {
		this.anotarAutoCommit = anotarAutoCommit;
	}

	public void setUsernameMap(Map<String, String> usernameMap) {
		this.usernameMap = usernameMap;
	}

	public void setPasswordMap(Map<String, String> passwordMap) {
		this.passwordMap = passwordMap;
	}

	public void setLoaded(boolean loaded) {
		this.loaded = loaded;
	}

	public void setCustomParams(Map<String, String> customParams) {
		this.customParams = customParams;
	}

	public void setScriptCache(Map<String, PyObject> scriptCache) {
		this.scriptCache = scriptCache;
	}

	public void setGroovyScriptCache(Map<String, String> groovyScriptCache) {
		this.groovyScriptCache = groovyScriptCache;
	}

	public void setConfigCache(Map<String, JsonSimpleConfig> configCache) {
		this.configCache = configCache;
	}

	public void setUseCache(boolean useCache) {
		this.useCache = useCache;
	}

	public void setMessaging(MessagingServices messaging) {
		this.messaging = messaging;
	}

	public void setSolrServerMap(Map<String, SolrServer> solrServerMap) {
		this.solrServerMap = solrServerMap;
	}

	public void setEngine(ScriptEngine engine) {
		this.engine = engine;
	}

	public SolrServer getSolr() {
		return solr;
	}

	public void setSolr(SolrServer solr) {
		this.solr = solr;
	}

	public void setStorage(Storage storage) {
		this.storage = storage;
	}

	/**
	 * Index a payload using the provided data using a python script
	 * 
	 * @param object
	 *            : The DigitalObject to index
	 * @param payload
	 *            : The Payload to index
	 * @param in
	 *            : Reader containing the new empty document
	 * @param inConf
	 *            : An InputStream holding the config file
	 * @param rulesOid
	 *            : The oid of the rules file to use
	 * @param props
	 *            : Properties object containing the object's metadata
	 * @return File : Temporary file containing the output to index
	 * @throws IOException
	 *             if there were errors accessing files
	 * @throws RuleException
	 *             if there were errors during indexing
	 */
	private String indexByPythonScript(DigitalObject object, Payload payload, String confOid, String rulesOid,
			Properties props) throws IOException, RuleException {
		try {
			JsonSimpleConfig jsonConfig = getConfigFile(confOid);

			// Get our data ready
			Map<String, Object> bindings = new HashMap<String, Object>();
			Map<String, List<String>> fields = new HashMap<String, List<String>>();
			bindings.put("fields", fields);
			bindings.put("jsonConfig", jsonConfig);
			bindings.put("indexer", this);
			bindings.put("object", object);
			bindings.put("payload", payload);
			bindings.put("params", props);
			bindings.put("pyUtils", getPyUtils());
			bindings.put("log", log);

			// Run the data through our script
			PyObject script = getPythonObject(rulesOid);
			if (script.__findattr__(SCRIPT_ACTIVATE_METHOD) != null) {
				script.invoke(SCRIPT_ACTIVATE_METHOD, Py.java2py(bindings));
				object.close();
			} else {
				log.warn("Activation method not found!");
			}

			return getPyUtils().solrDocument(fields);
		} catch (Exception e) {
			throw new RuleException(e);
		}
	}

	/**
	 * Evaluate the rules file stored under the provided object ID. If caching
	 * is configured the compiled python object will be cached to speed up
	 * subsequent access.
	 * 
	 * @param oid
	 *            : The rules OID to retrieve if cached
	 * @return PyObject : The cached object, null if not found
	 */
	private PyObject getPythonObject(String oid) {
		// Try the cache first
		PyObject rulesObject = deCachePythonObject(oid);
		if (rulesObject != null) {
			return rulesObject;
		}
		// We need to evaluate then
		InputStream inStream;
		if (oid.equals(ANOTAR_RULES_OID)) {
			// Anotar rules
			inStream = getClass().getResourceAsStream("/anotar.py");
			log.debug("First time parsing rules script: 'ANOTAR'");
			rulesObject = evalScript(inStream, "anotar.py");
			cachePythonObject(oid, rulesObject);
			return rulesObject;

		} else {
			// A standard rules file
			DigitalObject object;
			Payload payload;
			try {
				object = storage.getObject(oid);
				String scriptName = object.getSourceId();
				payload = object.getPayload(scriptName);
				inStream = payload.open();
				log.debug("First time parsing rules script: '{}'", oid);
				rulesObject = evalScript(inStream, scriptName);
				payload.close();
				cachePythonObject(oid, rulesObject);
				return rulesObject;
			} catch (StorageException ex) {
				log.error("Rules file could not be retrieved! '{}'", oid, ex);
				return null;
			}
		}
	}

	/**
	 * Retrieve and parse the config file stored under the provided object ID.
	 * If caching is configured the instantiated config object will be cached to
	 * speed up subsequent access.
	 * 
	 * @param oid
	 *            : The config OID to retrieve from storage or cache
	 * @return JsonSimple : The parsed or cached JSON object
	 */
	private JsonSimpleConfig getConfigFile(String oid) {
		if (oid == null) {
			return null;
		}

		// Try the cache first
		JsonSimpleConfig configFile = deCacheConfig(oid);
		if (configFile != null) {
			return configFile;
		}
		// Or evaluate afresh
		try {
			DigitalObject object = storage.getObject(oid);
			Payload payload = object.getPayload(object.getSourceId());
			log.debug("First time parsing config file: '{}'", oid);
			configFile = new JsonSimpleConfig(payload.open());
			payload.close();
			cacheConfig(oid, configFile);
			return configFile;
		} catch (IOException ex) {
			log.error("Rules file could not be parsed! '{}'", oid, ex);
			return null;
		} catch (StorageException ex) {
			log.error("Rules file could not be retrieved! '{}'", oid, ex);
			return null;
		}
	}

	/**
	 * Evaluate and return a Python script.
	 * 
	 * @param inStream
	 *            : InputStream containing the script to evaluate
	 * @param scriptName
	 *            : filename of the script (mainly for debugging)
	 * @return PyObject : Compiled result
	 */
	private PyObject evalScript(InputStream inStream, String scriptName) {
		// Execute the script
		PythonInterpreter python = new PythonInterpreter();
		python.execfile(inStream, scriptName);
		// Get the result and cleanup
		PyObject scriptClass = python.get(SCRIPT_CLASS_NAME);
		python.cleanup();
		// Instantiate and return the result
		return scriptClass.__call__();
	}

	/**
	 * Add a python object to the cache if caching if configured
	 * 
	 * @param oid
	 *            : The rules OID to use as an index
	 * @param pyObject
	 *            : The compiled PyObject to cache
	 */
	private void cachePythonObject(String oid, PyObject pyObject) {
		if (useCache && pyObject != null) {
			scriptCache.put(oid, pyObject);
		}
	}

	/**
	 * Return a python object from the cache if configured
	 * 
	 * @param oid
	 *            : The rules OID to retrieve if cached
	 * @return PyObject : The cached object, null if not found
	 */
	private PyObject deCachePythonObject(String oid) {
		if (useCache && scriptCache.containsKey(oid)) {
			return scriptCache.get(oid);
		}
		return null;
	}

	/**
	 * Add a config class to the cache if caching if configured
	 * 
	 * @param oid
	 *            : The config OID to use as an index
	 * @param config
	 *            : The instantiated JsonConfigHelper to cache
	 */
	private void cacheConfig(String oid, JsonSimpleConfig config) {
		if (useCache && config != null) {
			configCache.put(oid, config);
		}
	}

	/**
	 * Return a config class from the cache if configured
	 * 
	 * @param oid
	 *            : The config OID to retrieve if cached
	 * @return JsonConfigHelper : The cached config, null if not found
	 */
	private JsonSimpleConfig deCacheConfig(String oid) {
		if (useCache && configCache.containsKey(oid)) {
			return configCache.get(oid);
		}
		return null;
	}

	public void shutdown() throws IndexerException {
		getPyUtils().shutdown();
	}

	public void queueMessage(String queueId, String message) throws MessagingException {
		getMessaging().queueMessage(SolrWrapperQueueConsumer.QUEUE_ID, message);
	}


}