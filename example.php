<?php
require_once __DIR__.'/vendor/autoload.php';
require_once __DIR__.'/src/MQueueService.php';

class Example {
    public $queueName = "example"; //your queue's name

    // push a message
    public function send() {
        $queueName = $this->queueName;
        $amqp_content = new MQueueService();
        $exp = 5;
        return $amqp_content->sendMessage($queueName,"this is a test...",$exp);
    }

    // receive a message
    public function receive() {
        $queueName = $this->queueName;
        $amqp_content = new MQueueService();
        return $amqp_content->receiveMessage($queueName);
    }
}

$ex = new Example();
//publish a message
$publish = $ex->send();
//receive a message
$receive = $ex->receive();

echo "your publish a message :";
print_r($publish);
echo PHP_EOL;
echo "your receive a message :".json_encode($receive);