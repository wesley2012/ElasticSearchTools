package wesley2012;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

import java.text.ParseException;
import java.util.Map;

/**
 * Created by gaolb on 9/6/17.
 */
public class CommandCopy extends Command {

    ElasticSearchURL srcElasticSearchURL;
    ElasticSearchURL dstElasticSearchURL;

    BulkRequestBuilder bulk = null;
    int requestSize = 0;
    int total = 0;
    boolean failed;

    @Override
    void help() {
        System.err.println("copy http://host1/index1/type1 http://host2/index2/type2");
    }

    @Override
    void parse(String[] args) throws Exception {
        if (args.length != 2){
            throw new RuntimeException("Bad arguments");
        }
        srcElasticSearchURL = ElasticSearchURL.valueOf(args[0]);
        if (srcElasticSearchURL.index == null){
            throw new RuntimeException("Source URL format: http://host/index/type or http://host/index");
        }
        dstElasticSearchURL = ElasticSearchURL.valueOf(args[1]);
        if (dstElasticSearchURL.index == null) {
            throw new RuntimeException("Target URL format: http://host/index/type or http://host/index");
        }
        if (srcElasticSearchURL.type == null && dstElasticSearchURL.type != null){
            throw new RuntimeException("Please specify the source index type");
        }
        if (srcElasticSearchURL.type != null && dstElasticSearchURL.type == null){
            dstElasticSearchURL.type = srcElasticSearchURL.type;
        }
    }

    @Override
    void exec() {
        String scrollId = null;
        SearchResponse response;
        SearchHit[] hits;

        while (true) {
            if (scrollId == null) {
                SearchRequestBuilder srb = srcElasticSearchURL.client.prepareSearch(srcElasticSearchURL.index)
                        .setSearchType(SearchType.SCAN)
                        .setScroll(TimeValue.timeValueMinutes(5))
                        .setSize(50);
                if (srcElasticSearchURL.type != null) {
                    srb.setTypes(srcElasticSearchURL.type);
                }
                response = srb.get();
                hits = response.getHits().getHits();
                scrollId = response.getScrollId();
                if (hits.length == 0){
                    continue;
                }
            }
            else {
                SearchScrollRequestBuilder ssrb = srcElasticSearchURL.client.prepareSearchScroll(scrollId)
                        .setScroll(TimeValue.timeValueMinutes(5));
                response = ssrb.get();
                hits = response.getHits().getHits();
                scrollId = response.getScrollId();
                if (hits.length == 0){
                    break;
                }
            }

            for (SearchHit hit: hits){
                Map<String, Object> source = hit.getSource();
                String id = hit.getId();
                String type = hit.getType();
                save(type, id, source);
            }
            flush();
        }
        if (scrollId != null){
            srcElasticSearchURL.client.prepareClearScroll().addScrollId(scrollId).get();
        }

        System.exit(failed ? 1 : 0);
    }

    void save(String type, String id, Map<String, Object> source){
        if (bulk == null) {
            bulk = dstElasticSearchURL.client.prepareBulk();
            requestSize = 0;
        }
        if (dstElasticSearchURL.type != null){
            type = dstElasticSearchURL.type;
        }
        bulk.add(dstElasticSearchURL.client.prepareIndex(dstElasticSearchURL.index, type, id).setSource(source));
        requestSize++;
        total++;
        if (requestSize == 50){
            flush();
        }
    }

    void flush(){
        if (bulk == null){
            return;
        }
        System.err.format("doc num %d\n", total);
        BulkResponse response = bulk.get();
        if (response.hasFailures()){
            System.err.println(response.buildFailureMessage());
            failed = true;
        }

        bulk = null;
        requestSize = 0;
    }
}
