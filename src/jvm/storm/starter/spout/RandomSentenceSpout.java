package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;

    /**
     * Spout使用open()方法提供的SpoutOutputCollector对象发射一个元祖到它的输出流
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    /**
     * storm通过调用该方法从Spout请求一个元祖。
     */
    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[] {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"};
        String sentence = sentences[_rand.nextInt(sentences.length)];
        _collector.emit(new Values(sentence));// Spout可以提供一个消息Id，便于识别元祖，调用该方法后，元祖被发送到Bolt
    }

    /**
     * 如果storm检查到一个元祖是完全处理的，storm将调用原Spout任务的ack()方法，把Spout提供给Storm的消息id作为输入参数
     */
    @Override
    public void ack(Object id) {
    }

    // 用途为：通过调用ack或者fail方法，可以把消息以及消息id一起发送给mq等，便于从mq等取出或者恢复。

    /**
     * 如果storm检查到一个元祖是超时的，storm将调用原Spout任务的fail()方法，把Spout提供给Storm的消息id作为输入参数
     */
    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
    
}