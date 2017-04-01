package com.googlecode.fascinator.indexer;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.Callable;

import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;

public class IndexPayloadThread implements Callable<AbstractMap.SimpleEntry<String,String>> {

	
	private DigitalObject object = null;
	private Payload payload;
	private SolrIndexHandler solrIndexHandler;


	public IndexPayloadThread(DigitalObject object, Payload payload, SolrIndexHandler solrIndexHandler) {
		this.object = object;
		this.payload = payload;
		this.solrIndexHandler = solrIndexHandler;
	}
	
	@Override
	public SimpleEntry<String, String> call() throws Exception {
		return solrIndexHandler.index(this.object, this.payload);
	}
	
	

}
