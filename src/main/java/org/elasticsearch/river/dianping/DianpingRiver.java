package org.elasticsearch.river.dianping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

import com.dianping.api.DealApi;

public class DianpingRiver extends AbstractRiverComponent implements River {
	
	private final ThreadPool threadPool;
    private final Client client;
    
    private String appKey = null;
    private String secret = null;
    
    private String apiType = null;
    private String city = null;
    
    private final String indexName;
    private final String typeName;
    
    private final int bulkSize;
    private final int maxConcurrentBulk;
    private final TimeValue bulkFlushInterval;
    
    private volatile BulkProcessor bulkProcessor;

    private volatile boolean closed = false;

    @SuppressWarnings("unchecked")
	@Inject
	protected DianpingRiver(RiverName riverName, RiverSettings settings, Client client, ThreadPool threadPool) {
		super(riverName, settings);
		this.client = client;
	    this.threadPool = threadPool;
	    
	    if (settings.settings().containsKey("dianping")) {
            Map<String, Object> dianpingSettings = (Map<String, Object>) settings.settings().get("dianping");
            
            if (dianpingSettings.containsKey("app")) {
            	Map<String, Object> app = (Map<String, Object>) dianpingSettings.get("app");
                if (app.containsKey("app_key")) {
                	appKey = XContentMapValues.nodeStringValue(app.get("app_key"), null);
                }
                if (app.containsKey("secret")) {
                	secret = XContentMapValues.nodeStringValue(app.get("secret"), null);
                }
            }
            apiType = XContentMapValues.nodeStringValue(dianpingSettings.get("api_type"), "deal");
            city = XContentMapValues.nodeStringValue(dianpingSettings.get("city"), "天水");
	    }
	    
	    logger.info("creating dianping api river");
	    
	    if (appKey == null || secret == null) {
            indexName = null;
            typeName = "deal";
            bulkSize = 100;
            this.maxConcurrentBulk = 1;
            this.bulkFlushInterval = TimeValue.timeValueSeconds(5);
            logger.warn("no oauth specified, disabling river...");
            return;
        }
	    
	    if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "deal");
            this.bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            this.bulkFlushInterval = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                    indexSettings.get("flush_interval"), "5s"), TimeValue.timeValueSeconds(5));
            this.maxConcurrentBulk = XContentMapValues.nodeIntegerValue(indexSettings.get("max_concurrent_bulk"), 1);
        } else {
            indexName = riverName.name();
            typeName = "deal";
            bulkSize = 100;
            this.maxConcurrentBulk = 1;
            this.bulkFlushInterval = TimeValue.timeValueSeconds(5);
        }
	}

	@Override
	public void start() {
		try {
            // We push ES mapping only if raw is false
			String mapping = Streams.copyToStringFromClasspath("/mapping.json");
            client.admin().indices().prepareCreate(indexName).addMapping(typeName, mapping).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }
		
		// Creating bulk processor
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.debug("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.debug("Executed bulk composed of {} actions", request.numberOfActions());
                if (response.hasFailures()) {
                    logger.warn("There was failures while executing bulk", response.buildFailureMessage());
                    if (logger.isDebugEnabled()) {
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                                logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                                        item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());
                            }
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
            }
        })
                .setBulkActions(bulkSize)
                .setConcurrentRequests(maxConcurrentBulk)
                .setFlushInterval(bulkFlushInterval)
                .build();
        
        try {
			startApi();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void startApi() throws IOException{
		//TODO API Factory -> Type Api
		List<String> dealCities = new ArrayList<String>();
    	if(StringUtils.isEmpty(city)){
    		dealCities = DealApi.requestCities(appKey, secret);
    	}else{
    		dealCities.add(city);
    	}
    	Map<String, List<String>> dealIds = DealApi.requestDealIds(appKey, secret, dealCities);
    	
        for (Entry<String, List<String>> entry : dealIds.entrySet())
        {
            city = entry.getKey();
            List<String> dealIdsWithinCity = entry.getValue();
            List<List<String>> batchIdsList = DealApi.splitList(dealIdsWithinCity, 40);
            for (List<String> batchIds : batchIdsList)
            {
            	List<String> jsonList = DealApi.requestBatchDeals(appKey, secret, batchIds);
            	for (String json : jsonList) {
            		bulkProcessor.add(Requests.indexRequest(indexName).type(typeName).create(true).source(json));
				}
                DealApi.sleepAWhile();
            }
        }
    	
	}

	@Override
	public void close() {
		this.closed = true;
        logger.info("closing dianping api river");

        bulkProcessor.close();
	}
	

}
