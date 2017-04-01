package com.elasticsearch.client;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import Note.Note;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

public class ElasticSearchClient {

	private static final String NOTES_TYPE_NAME = "notes";
    private static final String DIARY_INDEX_NAME = "diary";
    
	public static void main (String[] args) {

		elasticApi();
		
		try {
            // Get Jest client
            HttpClientConfig clientConfig = new HttpClientConfig.Builder(
                    "http://localhost:9200").multiThreaded(true).build();
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(clientConfig);
            JestClient jestClient = factory.getObject();

            try {
                // run test index & searching
				ElasticSearchClient.deleteTestIndex(jestClient);
                ElasticSearchClient.createTestIndex(jestClient);
                ElasticSearchClient.indexSomeData(jestClient);
                ElasticSearchClient.readAllData(jestClient);
            } finally {
                // shutdown client
                jestClient.shutdownClient();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void elasticApi() {
    	TransportClient client = null;
    	try {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		
		
		IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
        .setSource(jsonBuilder()
                    .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                    .endObject()
                  )
		        .get();
		System.out.println("Test" + response);
		
		// Index name
		String _index = response.getIndex();
		// Type name
		String _type = response.getType();
		// Document ID (generated or not)
		String _id = response.getId();
		// Version (if it's the first time you index this document, you will get: 1)
		long _version = response.getVersion();
		// isCreated() is true if the document is a new one, false if it has been updated
		//boolean created = response.isCreated();
		
		
		GetResponse response1 = client.prepareGet("twitter", "tweet", "1").get();
		System.out.println("Test" + response1);
		
		UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1")
			        .doc(jsonBuilder()
			            .startObject()
			            .field("user", "Naren")
			            .endObject());
		client.update(updateRequest).get();
	
		GetResponse response3 = client.prepareGet("twitter", "tweet", "1").get();
		System.out.println("Test" + response3);
	
		//DeleteResponse response2 = client.prepareDelete("twitter", "tweet", "1").get()
		       
		//System.out.println("Test" + response2);
		
		IndexRequest indexRequest = new IndexRequest("index", "type", "1")
		        .source(jsonBuilder()
		            .startObject()
		                .field("name", "Joe Smith")
		                .field("gender", "male")
		            .endObject());
		
		updateRequest = new UpdateRequest("index", "type", "1")
		        .doc(jsonBuilder()
		            .startObject()
		                .field("gender", "fewmale")
		            .endObject())
		        .upsert(indexRequest);              
		//client.update(updateRequest).get();
		GetResponse response4 = client.prepareGet("index", "type", "1").get();
		System.out.println("Test" + response4);
		
	} catch (Exception  e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
	}

	private static void createTestIndex(final JestClient jestClient)
            throws Exception {

        // create new index (if u have this in elasticsearch.yml and prefer
        // those defaults, then leave this out
        Settings.Builder settings = Settings.builder();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 0);
        jestClient.execute(new CreateIndex.Builder(DIARY_INDEX_NAME).build());
    }

    private static void readAllData(final JestClient jestClient)
            throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("note", "see"));

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(DIARY_INDEX_NAME).addType(NOTES_TYPE_NAME).build();
        System.out.println(searchSourceBuilder.toString());
        JestResult result = jestClient.execute(search);
        List<Note> notes = result.getSourceAsObjectList(Note.class);
        for (Note note : notes) {
            System.out.println(note);
        }
    }

    private static void deleteTestIndex(final JestClient jestClient)
            throws Exception {
        DeleteIndex deleteIndex = new DeleteIndex.Builder(DIARY_INDEX_NAME)
                .build();
        jestClient.execute(deleteIndex);
    }

    private static void indexSomeData(final JestClient jestClient)
            throws Exception {
        // Blocking index
        final Note note1 = new Note("mthomas", "Note1: do u see this - "
                + System.currentTimeMillis());
        Index index = new Index.Builder(note1).index(DIARY_INDEX_NAME)
                .type(NOTES_TYPE_NAME).build();
        jestClient.execute(index);

        // Asynch index
        final Note note2 = new Note("mthomas", "Note2: do u see this - "
                + System.currentTimeMillis());
        index = new Index.Builder(note2).index(DIARY_INDEX_NAME)
                .type(NOTES_TYPE_NAME).build();
        jestClient.executeAsync(index, new JestResultHandler<JestResult>() {
            public void failed(Exception ex) {
            }

            public void completed(JestResult result) {
                note2.setId((String) result.getValue("_id"));
                System.out.println("completed==>>" + note2);
            }
        });

        // bulk index
        final Note note3 = new Note("mthomas", "Note3: do u see this - "
                + System.currentTimeMillis());
        final Note note4 = new Note("mthomas", "Note4: do u see this - "
                + System.currentTimeMillis());
        Bulk bulk = new Bulk.Builder()
                .addAction(
                        new Index.Builder(note3).index(DIARY_INDEX_NAME)
                                .type(NOTES_TYPE_NAME).build())
                .addAction(
                        new Index.Builder(note4).index(DIARY_INDEX_NAME)
                                .type(NOTES_TYPE_NAME).build()).build();
        JestResult result = jestClient.execute(bulk);

        Thread.sleep(2000);

        System.out.println(result.toString());
    }
}
