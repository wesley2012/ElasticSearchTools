package wesley2012;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gaolb on 8/26/17.
 */
public class ElasticSearchTools {

    static TransportClient createClientFromURL(String url) throws Exception {
        URL urlObj = new URL(url);
        String urlPath = urlObj.getPath();
        String urlPrefix = url.substring(0, url.length() - urlPath.length());

        HttpClient httpClient = HttpClients.createDefault();
        HttpGet get = new HttpGet(urlPrefix + "/_nodes");
        HttpResponse response = httpClient.execute(get);

        String text = EntityUtils.toString(response.getEntity(), "UTF-8");

        JSONObject jsonObject = JSON.parseObject(text);

        String clusterName = jsonObject.getString("cluster_name");

        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
        builder.put("cluster.name", clusterName);
        builder.put("client.transport.sniff", true);

        TransportClient client = new TransportClient(builder.build());

        Pattern pattern = Pattern.compile("inet\\[/([0-9\\.]+):([0-9]+)\\]");
        JSONObject nodes = jsonObject.getJSONObject("nodes");
        for (String key: nodes.keySet()){
            JSONObject node = nodes.getJSONObject(key);
            String addrLine = node.getString("transport_address");
            Matcher m = pattern.matcher(addrLine);
            if (!m.find()){
                System.err.println(String.format("Unable to parse address line '%s'", addrLine));
                System.exit(1);
            }
            String host = m.group(1);
            int port = Integer.parseInt(m.group(2));
            client.addTransportAddresses(new InetSocketTransportAddress(host, port));
        }

        return client;
    }

    static void clear(String url) throws Exception {
        URL urlObj = new URL(url);
        String urlPath = urlObj.getPath();
        Path path = Paths.get(urlPath);
        if (path.getNameCount() != 2){
            System.err.println("Invalid URL");
            System.exit(1);
        }

        TransportClient client = createClientFromURL(url);

        String index = path.getName(0).toString();
        String type = path.getName(1).toString();

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

    public static void main(String[] args) throws Exception {

        if (args.length < 1){
            System.err.println("Example:");
            System.err.println("\tclear http://host/index/type");
            System.exit(1);
        }


        String cmd = args[0];

        if (cmd.equals("clear")){
            if (args.length != 2){
                System.err.println("Missing URL");
                System.exit(1);
            }
            clear(args[1]);
        }
        else {
            System.err.println("Unsupported command");
            System.exit(1);
        }
    }
}
