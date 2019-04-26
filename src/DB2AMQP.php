<?php

namespace HiQDev\DB2AMQP;

use PDO;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class DB2AMQP
{
    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @var AMQPStreamConnection
     */
    protected $amqp;

    public function __construct(PDO $pdo, AMQPStreamConnection $amqp)
    {
        $this->pdo = $pdo;
        $this->amqp = $amqp;
    }

    public function run(string $dbChannel, string $exchange)
    {
        $this->listen($dbChannel);

        while (1) {
            $message = $this->getNotify();
            $this->publish($exchange, $message);
        }
    }

    protected function listen(string $dbChannel): void
    {
        $quoted = pg_escape_identifier($dbChannel);
        $this->pdo->exec("LISTEN $quoted");
    }

    protected function getNotify(): string
    {
        $data = $pdo->pgsqlGetNotify(PDO::FETCH_ASSOC, 10000); 

        return $data['payload'];
    }

    protected $channels = [];

    protected function getChannel($exchange): string
    {
        if (empty($this->channels[$exchange])) {
            $this->channels[$exchange] = $this->createChannel($exchange);
        }

        return $this->channels[$exchange];
    }

    protected function createChannel(string $exchange): AMQPChannel
    {
        $channel = $this->amqp->channel();
        $channel->exchange_declare($exchange, 'fanout', false, false, false);
    }

    protected function publish($message, $exchange, $routing): void
    {
        $channel = $this->getChannel($exchange);
        $channel->basic_publish($message, $exchange, $routing);
    }
}
