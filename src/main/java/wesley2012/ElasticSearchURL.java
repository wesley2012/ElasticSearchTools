package wesley2012;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gaolb on 9/6/17.
 */
public class ElasticSearchURL {
    TransportClient client;
    String index;
    String type;

    public static ElasticSearchURL valueOf(String url) throws Exception{
        ElasticSearchURL esURL = new ElasticSearchURL();
        URL urlObj = new URL(url);
        String urlPath = urlObj.getPath();
        String urlPrefix = url.substring(0, url.length() - urlPath.length());

        Path path = Paths.get(urlPath);
        if (path.getNameCount() > 0) {
            esURL.index = path.getName(0).toString();
            if (path.getNameCount() > 1) {
                esURL.type = path.getName(1).toString();
            }
        }

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

        esURL.client = client;

        return esURL;
    }

    public TransportClient getClient() {
        return client;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }
}
