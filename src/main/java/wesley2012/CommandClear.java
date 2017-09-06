package wesley2012;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by gaolb on 9/6/17.
 */
public class CommandClear extends Command {

    ElasticSearchURL esURL;

    @Override
    void help() {
        System.err.println("clear http://host/index/type");
    }

    @Override
    void parse(String[] args) throws Exception {
        if (args.length != 1){
            throw new RuntimeException("Bad arguments");
        }
        esURL = ElasticSearchURL.valueOf(args[0]);
        if (esURL.index == null || esURL.type == null){
            throw new RuntimeException("URL format: http://host/index/type");
        }
    }

    @Override
    void exec() {
        TransportClient client = esURL.client;
        String index = esURL.index;
        String type = esURL.type;

        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = client.prepareDeleteByQuery(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery());
        DeleteByQueryResponse response = deleteByQueryRequestBuilder.get();
        if (response.status().getStatus() == 200){
            System.err.println("OK");
        }
        else {
            System.err.println("Status: " + response.status().getStatus());
            System.exit(1);
        }
    }
}
