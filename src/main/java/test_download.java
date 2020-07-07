import org.apache.commons.cli.*;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.common.unit.TimeValue;


import java.io.*;

public class test_download {
    public static void main(String args[]) throws  Exception{

        String index = null;
        String field[] = null;
        String filename;
        String ip = null;
        String port = null;
        Options options = new Options();
        options.addOption("help",false,"打印帮助信息");
        options.addOption("config",true,"指定配置文件路径");
        options.addOption("ip",true,"es集群任意节点的地址");
        options.addOption("port",true,"es节点端口");
        options.addOption("index",true,"es中数据存储的索引");
        options.addOption("file",true,"指定输出文件路径(默认在当前路径 文件名由 index+type构成)");
        options.addOption("field",true,"指定需要从es中下载信息包含的字段（默认为全量下载）多个字段逗号隔开");

        CommandLine cmd = null;
        CommandLineParser parser = new DefaultParser();
        try{
            cmd = parser.parse(options,args);
        }catch (Exception e){
            e.printStackTrace();
        }

        HelpFormatter formatter = new HelpFormatter();
        if(cmd.hasOption("help")){
            formatter.printHelp("java -jar ESTool.jar",options);
            System.exit(0);
        }

        if(cmd.hasOption("ip")){
            ip = cmd.getOptionValue("ip");
        }else{
            System.out.println("缺少参数 ip");
            formatter.printHelp("java -jar ESDownLoad.jar",options);
            System.exit(0);
        }

        if(cmd.hasOption("port")){
            port = cmd.getOptionValue("port");
        }else{
            System.out.println("缺少参数port");
            formatter.printHelp("java -jar ESTool.jar",options);
            System.exit(0);
        }

        if(cmd.hasOption("index")){
            index = cmd.getOptionValue("index");
        }else {
            System.out.println("缺少参数 index");
            formatter.printHelp("java -jar ESDownLoad.jar", options);
            System.exit(0);
        }


        filename = index + ".txt";

        if (cmd.hasOption("file")) {
            filename = cmd.getOptionValue("file");
        }

        if (cmd.hasOption("field")) {
            String test = cmd.getOptionValue("field");
            field = test.split(",");
        }


//        String keyStorePass = "password";
//        File directory = new File("");
//        String projectPath = null;
//        try {
//            projectPath = directory.getCanonicalPath() + "/config/my_keystore.jks";
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        Path keyStorePath = Paths.get(projectPath);
//        KeyStore truststore = KeyStore.getInstance("jks");
//        try (InputStream is = Files.newInputStream(keyStorePath)) {
//            try {
//                truststore.load(is, keyStorePass.toCharArray());
//            } catch (CertificateException e) {
//                e.printStackTrace();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, new TrustSelfSignedStrategy());
//        final SSLContext sslContext = sslBuilder.build();

//        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("bishi", "bishi2019"));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(ip, 9200, "http")).setMaxRetryTimeoutMillis(10*60*1000);
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(
//                            HttpAsyncClientBuilder httpClientBuilder) {
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
//                    }
//                });
        RestHighLevelClient client = new RestHighLevelClient(builder);

        System.out.println("client init successfully");
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(field,null);
        searchSourceBuilder.size(10000);
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        QueryBuilder[] queryBuilder = new QueryBuilder[100];
        if(cmd.hasOption("config")){
            String input = cmd.getOptionValue("config");
            int count = 0;
            try{
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(input))));
                String line;
                while((line = br.readLine()) != null){
                    String filed_name = line.split(":",2)[0];
                    String value_all = line.split(":",2)[1];
                    String[] value = value_all.split(",");
                    if(value.length == 1){
                        queryBuilder[count] = QueryBuilders.termQuery(filed_name,value[0]);
                        System.out.println("now we have termQuery for " + filed_name + " = " + value[0]);
                    }
                    else if(value.length == 2){
                        String s = value[0];
                        String e = value[1];
                        queryBuilder[count] = QueryBuilders.rangeQuery(filed_name).from(s).includeLower(true).to(e).includeUpper(true);
                        System.out.println("now we hava rangeQuery for " + filed_name + " from " + s + " to " + e);
                     }
                    else{
                        System.out.println("配置文件参数错误");
                        System.exit(0);
                    }
                    count = count + 1;
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            for(int i = 0; i < count; i++){
                qb.must(queryBuilder[i]);
            }
            searchSourceBuilder.query(qb);
        }
        //searchSourceBuilder.query(matchQuery("name", "shenyun"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.scroll(scroll);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        System.out.println("all messages count is " + searchResponse.getHits().getTotalHits());
        int count = 0;
        File file = new File(filename);
        try{
            FileOutputStream fop = new FileOutputStream(file);
            byte[] contentInBytes;
            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {
                    contentInBytes = (hit.getSourceAsString() + "\n").getBytes();
                    fop.write(contentInBytes);
                    count = count + 1;
                    if(count / 100000 > 0 && count % 100000 == 0){
                        System.out.println("now we have finished " + count + " messages");
                    }
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
            fop.flush();
            fop.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        if (succeeded) {
            System.out.println("clear scroll succeed");
        }
        client.close();
    }
}
