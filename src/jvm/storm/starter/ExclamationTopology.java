package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {
    
    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }


    }
    
    public static void main(String[] args) throws Exception {
        // 构建拓扑的类，用于指定执行的拓扑。拓扑底层是Thrift结构
        TopologyBuilder builder = new TopologyBuilder();

        // setSpout或setBolt中parallelismHint表示每个组件产生多少个执行器
        builder.setSpout("word", new TestWordSpout(), 10);
        // setNumTasks是热舞的初始数量，如果不显示配置，storm默认每个执行器执行一个任务
        builder.setBolt("exclaim1", new ExclamationBolt(), 3)/*.setNumTasks(4)*/
                .shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim1");
                
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            // 描述：集群中不同节点的拓扑可以创建多少个工作进程；配置选项：TOPOLOGY_WORKERS
            conf.setNumWorkers(3);// 使用3个工作进程
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }
}
