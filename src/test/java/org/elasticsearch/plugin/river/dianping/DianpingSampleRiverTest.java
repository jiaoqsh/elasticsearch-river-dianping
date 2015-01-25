package org.elasticsearch.plugin.river.dianping;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class DianpingSampleRiverTest {
	private static Logger logger = LogManager.getLogger(DianpingSampleRiverTest.class);
	
	protected long duration = 1000;
	private volatile BulkProcessor bulkProcessor;
	private final int bulkSize = 100;
    private final int maxConcurrentBulk = 1;
    private final TimeValue bulkFlushInterval = TimeValue.timeValueSeconds(5);

	public static void main(String[] args) throws InterruptedException, IOException {
		DianpingSampleRiverTest instance = new DianpingSampleRiverTest();
		instance.launcher(args);
	}
	
	 protected XContentBuilder riverSettings() throws IOException {
	        XContentBuilder xb = XContentFactory.jsonBuilder()
	            .startObject()
	                .field("type", "dianping")
	                .startObject("dianping")
	                    .startObject("oauth")
	                        .field("consumer_key", "key")
	                        .field("consumer_secret", "secret")
	                        .field("access_token", "token")
	                        .field("access_token_secret", "secret")
	                    .endObject();

	        // We inject specific test settings here
	        //xb = addSpecificRiverSettings(xb);
	        xb.field("type", "sample");

	        xb.endObject().endObject();
	        return xb;
	    }
	
	public void launcher(String[] args) throws InterruptedException, IOException {
		// First we delete old datas...
		logger.info("delete old datas...");
        File dataDir = new File("./target/es/data");
        if(dataDir.exists()) {
            FileSystemUtils.deleteRecursively(dataDir, true);
        }

        // Then we start our node for tests
        logger.info("start node for tests...");
        Node node = NodeBuilder
                .nodeBuilder()
                .local(true)
                .settings(
                        ImmutableSettings.settingsBuilder()
                                .put("gateway.type", "local")
                                .put("path.data", "./target/es/data")
                                .put("path.logs", "./target/es/logs")
                                .put("path.work", "./target/es/work")
                ).node();

        // We wait now for the yellow (or green) status
        logger.info("wait now for the yellow (or green) status...");
        node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        // We clean existing rivers
        logger.info("clean existing rivers...");
        try {
            node.client().admin().indices()
                    .delete(new DeleteIndexRequest("_river")).actionGet();
            // We wait for one second to let ES delete the river
            Thread.sleep(1000);
        } catch (IndexMissingException e) {
            // Index does not exist... Fine
        }
        logger.info("prepareIndex...");
        final String indexName = "_river";
        final String typeName = "dianping";
        final Client client = node.client();
       /* String mapping = XContentFactory.jsonBuilder().startObject().startObject(typeName).startObject("properties")
                .startObject("location").field("type", "geo_point").endObject()
                .startObject("language").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("user").startObject("properties").startObject("screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                .startObject("mention").startObject("properties").startObject("screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                .startObject("in_reply").startObject("properties").startObject("user_screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                .endObject().endObject().endObject().string();*/
        logger.info("mapping...");
       /* String mapping = XContentFactory.jsonBuilder().startObject().startObject(typeName).startObject("properties")
        		.startObject("title").field("type", "string").field("index", "not_analyzed").endObject()
        		.endObject().endObject().endObject().string();*/
        
        String mapping = Streams.copyToStringFromClasspath("/mapping.json");
        
        client.admin().indices().prepareCreate(indexName).addMapping(typeName, mapping).execute().actionGet();
        
        /*logger.info("index...");
        String titleJson = XContentFactory.jsonBuilder().startObject().field("title", "food").endObject().string();
        client.prepareIndex(indexName, typeName).setSource(titleJson).execute().actionGet();*/
        
        // Creating bulk processor
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("Going to execute new bulk composed of {} actions "+ request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.debug("Executed bulk composed of {} actions"+ request.numberOfActions());
                if (response.hasFailures()) {
                    logger.warn("There was failures while executing bulk"+ response.buildFailureMessage());
                    if (logger.isDebugEnabled()) {
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                               /* logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                                        item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());*/
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
        
        bulkProcessor.add(Requests.indexRequest(indexName).type(typeName).create(true).source(getData()));
        
        Thread.sleep(duration * 1000);
        
        // We remove the river as well. Not mandatory here as the JVM will stop
        // but it's an example on how to remove a running river (and call close() method).
        logger.info("remove the river as well...");
        node.client().admin().indices().prepareDeleteMapping(indexName).setType(typeName).execute().actionGet();
        
        
	}
	
	private String getData() {
		try {
			return XContentFactory.jsonBuilder().startObject()
					.field("title", "food bar").field("desc","xxxxxx")
					.field("telephone","021-60671611").field("deal_count", 3).endObject().string();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}
	
}
