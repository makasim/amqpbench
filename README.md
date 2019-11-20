# AMQP Bench

A tool for generating some rate-limited traffic to RabbitMQ queue.

## Use

Install:
```
go get github.com/makasim/amqpbench
```

Run:
```
amqpbench -rate=10 -payload="msgBody" -queue=queue-to-send -reply-queue=reply-queue -amqp="amqp://guest:guest@localhost:5672//" -rate=50 -port=3000
``` 

Change Rate
```
curl -X GET http://localhost:3000/rate/100
``` 

## Licence

MIT
