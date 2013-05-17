/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.rest;

import java.io.Closeable;
import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.Writable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;

/**
 * Rest client performing high-level operations using buffers to improve performance. Stateful in that once created, it is used to perform updates against the same index.
 */
public class BufferedRestClient implements Closeable {

    // TODO: make this configurable
    private final byte[] buffer;
    private final int bufferEntriesThreshold;
	private static final Logger LOG = Logger.getLogger(BufferedRestClient.class);

    private int bufferSize = 0;
    private int bufferEntries = 0;
    private boolean requiresRefreshAfterBulk = false;
    private boolean executedBulkWrite = false;

    private ObjectMapper mapper = new ObjectMapper();

    private String operationType;
    private String idField;
    private RestClient client;
    private String index;

    public BufferedRestClient(Settings settings) {
        this.client = new RestClient(settings);
        this.index = settings.getTargetResource();
        this.operationType = settings.getOperationType();
        this.idField = settings.getIDField();
        buffer = new byte[settings.getBatchSizeInBytes()];
        bufferEntriesThreshold = settings.getBatchSizeInEntries();
        requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();
    }

    /**
     * Returns a pageable result to the given query.
     *
     * @param uri
     * @return
     */
    public QueryResult query(String uri) {
        return new QueryResult(client, uri);
    }
    
    
    public void executeESBulk(Object object) throws IOException {
    	LOG.info("executeESBulk - Operation type " + this.operationType);
    	if(this.operationType == "index")
    	{
    		addToIndex(object);
    	}
    	else
    	{
    		deleteFromIndex(object);
    	} 	
    }
    
    
    /**
     * Writes the objects to index.
     *
     * @param index
     * @param object
     */
    public void addToIndex(Object object) throws IOException {
	    LOG.info("addToIndex");
        Validate.notEmpty(index, "no index given");
        
        if(this.idField != "_id")
        {
          Object d = (object instanceof Writable ? WritableUtils.fromWritable((Writable) object) : object);
          Object rid   = ((LinkedHashMap)d).get("rid");
        }
        
        LOG.info("Writable" + d.toString());
        
        StringBuilder sb = new StringBuilder();
        //Writting rid as ID
        if(this.idField == "_id")
        {
            sb.append("{\"index\":{}}\n");
        }
        else
        {
            sb.append("{\"index\":{\"_id\":\""+ rid.toString() + "\"}}\n");
        }
        sb.append(getESQuery(d));
        sb.append("\n");
        
        LOG.info("ES index query" + sb.toString());

        byte[] data = sb.toString().getBytes("UTF-8");

        // make some space first
        if (data.length + bufferSize >= buffer.length) {
            flushBatch();
        }

        System.arraycopy(data, 0, buffer, bufferSize, data.length);
        bufferSize += data.length;
        bufferEntries++;

        if (bufferEntriesThreshold > 0 && bufferEntries >= bufferEntriesThreshold) {
            flushBatch();
        }
    }

	private String getESQuery(Object d) throws IOException,
			JsonGenerationException, JsonMappingException {
		String rdata = ((LinkedHashMap)d).get("rdata").toString();
        
        ((LinkedHashMap)d).remove("rdata");
        
        rdata = rdata.substring(1,rdata.length() -1);
 
        String[] maps = rdata.split(",");
       
        String result = "";
        
        for(String map : maps)
        {
        	 String[] array = map.split("=");
        	 if(result.length() > 2) result = result + ",";
        	 result = result + "{\"mapid\":" +  array[0] + ",\"value\":\"" + array[1] + "\"}";      	
        }
        
      //String rdataStr = String.format("[{0}]", result);
        
        String ESQuery = mapper.writeValueAsString(d);
        ESQuery = ESQuery.substring(0,ESQuery.length() -1).concat(", \"rdata\":[" + result + "]}");
		return ESQuery;
	}
    
    
    /**
     * Deletes the objects to index.
     *
     * @param index
     * @param object
     */
    public void deleteFromIndex(Object object) throws IOException {
        LOG.info("deleteFromIndex");

        Object d = (object instanceof Writable ? WritableUtils.fromWritable((Writable) object) : object);
        
        if(this.idField == "_id")
        {
            throw new IOException("Id field is not specified");
        }

        StringBuilder sb = new StringBuilder();
        
        Object rid = ((LinkedHashMap)d).get("rid");
        Object esid = "_id";
        ((LinkedHashMap)d).put(esid, rid);
        
        sb.append("{\"delete\":");
        sb.append(mapper.writeValueAsString(d));
        sb.append("}\n");
               
        LOG.info("ES delete query" + sb.toString());
        LOG.info(sb.toString());
        
        byte[] data = sb.toString().getBytes("UTF-8");

        // make some space first
        if (data.length + bufferSize >= buffer.length) {
            flushBatch();
        }

        System.arraycopy(data, 0, buffer, bufferSize, data.length);
        bufferSize += data.length;
        bufferEntries++;

        if (bufferEntriesThreshold > 0 && bufferEntries >= bufferEntriesThreshold) {
            flushBatch();
        }
    }

    private void flushBatch() throws IOException {
        client.bulk(index, buffer, bufferSize);
        bufferSize = 0;
        bufferEntries = 0;
        executedBulkWrite = true;
    }

    @Override
    public void close() throws IOException {
        if (bufferSize > 0) {
            flushBatch();
        }
        if (requiresRefreshAfterBulk && executedBulkWrite) {
            // refresh batch
            client.refresh(index);
        }
        client.close();
    }
   
 }