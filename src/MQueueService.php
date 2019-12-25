<?php
/**
 * @desc  Delay Operate RabbitMQ
 * @author lemy
 * @time 2019-12-17 13:06:35
 */

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class MQueueService
{
    # 链接主机
    public $host = 'localhost';
    # 主机链接端口
    public $port = 5672;
    # 主机登陆用户名
    public $username = 'guest';
    # 主机登陆密码
    public $password = 'guest';
    # 虚拟主机
    public $vhost = '';
    # 链接对象实例
    public $connection = null;
    # 频道
    public $channel = null;
    # 延迟交换机类型
    public $type = 'direct';
    # 日志文件名称
    private $log_file = 'amqp-sys.log';
    # 返回值
    private $receive = "";

    /**
     * 一个延时推送消息服务
     * MQueueService constructor.
     */
    public function __construct()
    {
        global $argc, $argv;
        $this->argc = $argc;
        $this->argv = $argv;
        $this->host = getenv('rbmq_sn_generate_host') ?? $this->host;
        $this->port = getenv('rbmq_sn_generate_port') ?? $this->port;
        $this->username = getenv('rbmq_sn_generate_username') ?? $this->username;
        $this->password = getenv('rbmq_sn_generate_password') ?? $this->password;
        $this->vhost = getenv('rbmq_sn_generate_vhost') ?? $this->vhost;
        $this->type = getenv('rbmq_sn_generate_type') ?? $this->type;

        $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->username, $this->password, $this->vhost);
        if (!$this->connection->isConnected()) {
            LogService::getLogger(['format' => '', 'log_file' => $this->log_file])->error('error info::Rabbitmq Connect service error');
            return false;
        }
        $this->channel = $this->connection->channel();
    }

    /**
     * 生产消息
     * @param string $queuename 消息队列名称
     * @param string $message 消息
     * @param string $exp 延时
     * @return bool
     * @throws Exception
     */
    public function sendMessage($queuename = "", $message = "", $exp = "10")
    {
        // 写入消息队列
        if (isset($exp) && $exp > 0) {
            $expiration = intval($exp) * 1000;
        } else {
            $expiration = 10;
        }

        //格式化消息
        $queuemsg = array(
            "messageId"     => date("Ymdhis") . rand(1000, 9999),
            "receiptHandle" => md5(time() . rand(100, 999)),
            "messageBody"   => $message,
            "createtime"    => time(),
            "exp"           => $expiration
        );

        $delay_exchange = $queuename . ".delay_exchange";
        $cache_exchange = $queuename . ".cache_exchange";
        $delay_queue = $queuename . '.delay_queue';
        $cache_queue = $queuename . '.cache_queue';

        $cache_exchange_name = $cache_exchange . $expiration;
        $cache_queue_name = $cache_queue . $expiration;
        $this->channel->exchange_declare($delay_exchange, $this->type, false, false, false);
        $this->channel->exchange_declare($cache_exchange_name, $this->type, false, false, false);

        $tale = new AMQPTable();
        $tale->set('x-dead-letter-exchange', $delay_exchange);
        $tale->set('x-dead-letter-routing-key', $delay_exchange);
        $tale->set('x-message-ttl', $expiration);
        $this->channel->queue_declare($cache_queue_name, false, true, false, false, false, $tale);
        $this->channel->queue_bind($cache_queue_name, $cache_exchange_name, '');
        $this->channel->queue_declare($delay_queue, false, true, false, false, false);
        $this->channel->queue_bind($delay_queue, $delay_exchange, $delay_exchange);
        $msg = new AMQPMessage(json_encode($queuemsg), array(
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ));
        $this->channel->basic_publish($msg, $cache_exchange_name, '');
        $this->close();
        return ["messageId" => $queuemsg["messageId"], "receiptHandle" => $queuemsg["receiptHandle"]];
    }

    /**
     * 消费队列
     * @param $queuename
     * @return array like {"messageId":"201912170221174488","receiptHandle":"d5be0af861b9e8c64f5e96ebc556a0ca","messageBody":"this is a test...1576563677"}
     * @throws ErrorException
     */
    public function receiveMessage($queuename)
    {
        $delay_exchange = $queuename . ".delay_exchange";
        $cache_exchange = $queuename . ".cache_exchange";
        $delay_queue = $queuename . '.delay_queue';
        $cache_queue = $queuename . '.cache_queue';

        $this->channel->exchange_declare($delay_exchange, $this->type, false, false, false);
        $this->channel->exchange_declare($cache_exchange, $this->type, false, false, false);

        $this->channel->queue_declare($delay_queue, false, true, false, false, false);
        $this->channel->queue_bind($delay_queue, $delay_exchange, $delay_exchange);

        $callback = function ($msg) {
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
            $this->receive = json_decode($msg->body, true);
        };

        //只有consumer已经处理并确认了上一条message时queue才分派新的message给它
        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($delay_queue, '', false, false, false, false, $callback);

        while (count($this->channel->callbacks)) {
            try{
                $this->channel->wait(null,false,0.1);
            } catch (Exception $e) {
                //超时接收无数据直接
                $this->close();
                return;
            }
            if ($this->receive != "") break; //获取到一条message需要重新发起消费队列
        }
        $exp = $this->receive["exp"];

        //当队列已空，删除队列清空交换机     Deletes a queue
        try {
            $this->channel->exchange_delete($delay_exchange);
            $this->channel->exchange_delete($cache_exchange);
            $this->channel->exchange_delete($cache_exchange . $exp);
            $this->channel->queue_delete($delay_queue, false, true);
            $this->channel->queue_delete($cache_queue . $exp, false, true);
        } catch (Exception $e) {
            //队列为非空，即将抛出处理异常
            $this->close();
            return;
        }
        $this->close();
        return $this->receive;
    }

    /**
     * 暂无方案
     */
    public function deleteMessage()
    {

    }

    /**
     * 关闭连接
     * @throws Exception
     */
    public function close()
    {
        $this->channel->close();
        $this->connection->close();
        return;
    }

}


