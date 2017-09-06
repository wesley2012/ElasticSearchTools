package wesley2012;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.ArrayUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gaolb on 8/26/17.
 */
public class ElasticSearchTools {
    public static void main(String[] args) throws Exception {
        Map<String, Command> cmds = new HashMap<>();
        cmds.put("clear", new CommandClear());
        cmds.put("copy", new CommandCopy());

        if (args.length < 1){
            System.err.println("Help:");
            for (Map.Entry<String, Command> e: cmds.entrySet()){
                e.getValue().help();
            }
            System.exit(1);
        }

        String cmdName = args[0];
        String[] cmdArgs = ArrayUtils.subarray(args, 1, args.length);

        Command cmd = cmds.get(cmdName);
        if (cmd == null){
            System.err.println("Unsupported command");
            System.exit(1);
        }
        cmd.parse(cmdArgs);
        cmd.exec();
    }
}
