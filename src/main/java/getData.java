//import org.apache.commons.cli.*;
//import org.elasticsearch.action.search.SearchRequestBuilder;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.index.query.BoolQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.sort.SortOrder;
//
//import java.io.*;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//
//
//public class getData {
//    public static void main(String args[]){
//
//        String host = null;
//        String port = "9300";
//        String cluster_name = "elasticsearch";
//        String index = null;
//        String type = null;
//        String filename;
//        String field[] = null;
//        Options options = new Options();
//        options.addOption("help",false,"打印帮助信息");
//        options.addOption("config",true,"指定配置文件路径");
//        options.addOption("ip",true,"es集群任意节点的地址");
//        options.addOption("port",true,"es集群client模式通信端口（默认为9300）");
//        options.addOption("cluster",true,"es集群名称（默认为elasticsearch）");
//        options.addOption("index",true,"es中数据存储的索引");
//        options.addOption("type",true,"es中数据存储的类型");
//        options.addOption("file",true,"指定输出文件路径(默认在当前路径 文件名由 index+type构成)");
//        options.addOption("field",true,"指定需要从es中下载信息包含的字段（默认为全量下载）多个字段逗号隔开");
//
//        CommandLine cmd = null;
//        CommandLineParser parser = new DefaultParser();
//        try{
//            cmd = parser.parse(options,args);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//
//        HelpFormatter formatter = new HelpFormatter();
//        if(cmd.hasOption("help")){
//            formatter.printHelp("java -jar ESDownLoad.jar",options);
//            System.exit(0);
//        }
//
//
//        if(cmd.hasOption("ip")){
//            host = cmd.getOptionValue("ip");
//        }else{
//            System.out.println("缺少参数 ip");
//            formatter.printHelp("java -jar ESDownLoad.jar",options);
//            System.exit(0);
//        }
//
//        if(cmd.hasOption("port")){
//            port = cmd.getOptionValue("port");
//        }
//
//        if(cmd.hasOption("cluster")){
//            cluster_name = cmd.getOptionValue("cluster");
//        }
//
//        if(cmd.hasOption("index")){
//            index = cmd.getOptionValue("index");
//        }else{
//            System.out.println("缺少参数 index");
//            formatter.printHelp("java -jar ESDownLoad.jar",options);
//            System.exit(0);
//        }
//
//        if(cmd.hasOption("type")){
//            type = cmd.getOptionValue("type");
//        }else {
//            System.out.println("缺少参数 type");
//            formatter.printHelp("java -jar ESDownLoad.jar",options);
//            System.exit(0);
//        }
//
//        filename = index + "_" + type + ".txt";
//
//        if(cmd.hasOption("file")){
//            filename = cmd.getOptionValue("file");
//        }
//
//        if(cmd.hasOption("field")){
//            String test = cmd.getOptionValue("field");
//            field = test.split(",");
//        }
//
////        TransportAddress addr = null;
////        try {
////            addr = new TransportAddress(InetAddress.getByName(host), Integer.parseInt(port));
////        } catch (UnknownHostException e) {
////            e.printStackTrace();
////        }
//        Settings settings = Settings.builder().put("cluster.name", cluster_name).put("client.transport.sniff", true).build();
////        //启动嗅探功能  只需要指定集群中的任意一个节点 就可以加载到集群中的其他节点
////        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(addr);
////
//        TransportClient client = null;
//        try{
//            client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),Integer.parseInt(port)));
//        } catch (UnknownHostException e){
//            e.printStackTrace();
//        }
//        SearchRequestBuilder search = client.prepareSearch(index).setTypes(type);
//        search.addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)  //输出结果无需排序
//                    .setScroll(new TimeValue(60000))
//                    .setSize(10000)    //es 通过java client的方式一次最多读取数据量为 1w条  需要做scroll游标的处理
//                    .setTimeout(new TimeValue(60000));
//
//        if(field != null){
//            search.setFetchSource(field,null);
//        }
//        BoolQueryBuilder qb = QueryBuilders.boolQuery();
//        QueryBuilder[] queryBuilder = new QueryBuilder[100];
//        if(cmd.hasOption("config")){
//            String input = cmd.getOptionValue("config");
//            int count = 0;
//            try{
//                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(input))));
//                String line;
//                while((line = br.readLine()) != null){
//                    String filed_name = line.split(":",2)[0];
//                    String value_all = line.split(":",2)[1];
//                    String[] value = value_all.split(",");
//                    if(value.length == 1){
//                        queryBuilder[count] = QueryBuilders.termQuery(filed_name,value[0]);
//                        System.out.println("now we have termQuery for " + filed_name + " = " + value[0]);
//                    }
//                    else if(value.length == 2){
//                        String s = value[0];
//                        String e = value[1];
//                        queryBuilder[count] = QueryBuilders.rangeQuery(filed_name).from(s).includeLower(true).to(e).includeUpper(true);
//                        System.out.println("now we hava rangeQuery for " + filed_name + " from " + s + " to " + e);
//                     }
//                    else{
//                        System.out.println("配置文件参数错误");
//                        System.exit(0);
//                    }
//                    count = count + 1;
//                }
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//            for(int i = 0; i < count; i++){
//                qb.must(queryBuilder[i]);
//            }
//            search.setQuery(qb);
//        }
//       // search.setQuery(QueryBuilders.termQuery("digested.raw","user=null"));
//        SearchResponse scrollResp = search.get();
//        System.out.println("during this time the number of data we get is " + scrollResp.getHits().totalHits());
//
//        File file = new File(filename);
//        try {
//            FileOutputStream fop = new FileOutputStream(file);
//            byte[] contentInBytes;
//            int count = 0;
//            do {
//                for (SearchHit hits : scrollResp.getHits().getHits()) {
//                    contentInBytes = (hits.getSourceAsString() + "\n").getBytes();
//                    fop.write(contentInBytes);
//                    count = count + 1;
//                    if(count / 10000 > 0 && count % 10000 == 0){
//                        System.out.println("now we have finished " + count + " messages");
//                    }
//                }
//                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).get();
//            } while (scrollResp.getHits().getHits().length != 0);
//            System.out.println("done");
//            fop.flush();
//            fop.close();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//}
