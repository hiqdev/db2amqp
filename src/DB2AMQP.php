<?php

namespace HiQDev\DB2AMQP;

use Closure;
use PDO;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;

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

    protected ?Closure $filter = null;

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

            $message = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
            if (!$this->shouldBePublished($message)) {

            }
            $this->publish($json, $exchange, $message['routing_key'] ?? '');
        }
    }

    protected function listen(string $dbChannel): void
    {
        /// TODO XXX ??? piece of shit
        /// $quoted = pg_escape_identifier($dbChannel);
        $escaped = preg_replace('/[^a-z0-9_]/', '', $dbChannel);
        if (!$escaped) {
            throw new RuntimeException("bad DB channel name: '$dbChannel'");
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

    protected function publish(string $json, string $exchange, ?string $routingKey = null): void
    {
        /** @noinspection ForgottenDebugOutputInspection */
        error_log(sprintf(
            'Publishing to exchange "%s" (routing-key %s) message "%s"',
            $exchange, $routingKey, $json
        ));

        $channel = $this->getChannel($exchange);
        $message = new AMQPMessage($json, [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'content_type' => 'application/json',
        ]);
        $channel->basic_publish($message, $exchange, $routingKey);
    }

    protected function sendHeartbeat(): void
    {
        $this->amqp->getIO()->read(0);
    }

    private function shouldBePublished($message): bool
    {
        if ($this->filter === null) {
            return true;
        }

        return call_user_func($this->filter, $message);
    }

    public function filterMessages(Closure $filter): void
    {
        $this->filter = $filter;
    }
}
