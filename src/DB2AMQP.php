<?php

namespace HiQDev\DB2AMQP;

use PDO;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

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
            $json = $this->getNotify();
            if ($json === null) {
                $this->sendHeartbeat();
                continue;
            }

            $data = json_decode($json, true);
            $this->publish($json, $exchange, $data['type'] ?? '');
        }
    }

    protected function listen(string $dbChannel): void
    {
        /// TODO XXX ??? piece of shit
        /// $quoted = pg_escape_identifier($dbChannel);
        $escaped = preg_replace('/[^a-z0-9_]/', '', $dbChannel);
        if (!$escaped) {
            throw new \Exception("bad DB channel name: '$dbChannel'");
        }
        $this->pdo->exec("LISTEN \"$escaped\"");
    }

    protected function getNotify(): ?string
    {
        $data = $this->pdo->pgsqlGetNotify(PDO::FETCH_ASSOC, 10000);
        if ($data === false) {
            return null;
        }

        return $data['payload'] ?? null;
    }

    protected $channels = [];

    protected function getChannel($exchange): AMQPChannel
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

        return $channel;
    }

    protected function publish(string $json, string $exchange, ?string $routing = null): void
    {
        error_log(sprintf(
            'Publishing to exchange "%s" (routing-key %s) message "%s"',
            $exchange, $routing, $json
        ));

        $channel = $this->getChannel($exchange);
        $message = new AMQPMessage($json, [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'content_type' => 'application/json',
        ]);
        $channel->basic_publish($message, $exchange, $routing);
    }

    protected function sendHeartbeat(): void
    {
        $this->amqp->getIO()->read(0);
    }
}
