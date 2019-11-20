package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/go-chi/chi"
	"github.com/juju/ratelimit"
	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

var amqpDsn string
var queue string
var port int
var payload string
var replyQueue string
var rate int

func main() {
	var wg sync.WaitGroup

	reqCh := make(chan int)
	respCh := make(chan int)

	rateCh := make(chan int, 1)
	rateCh <- rate

	wg.Add(1)
	go func(rateCh chan<- int) {
		defer wg.Done()

		r := chi.NewRouter()
		r.Get("/rate/{rate}", func(w http.ResponseWriter, r *http.Request) {
			rate, err := strconv.Atoi(chi.URLParam(r, "rate"))
			if err != nil {
				w.Write([]byte(err.Error()))

				return
			}
			if rate < 1 {
				w.Write([]byte("rate is less then zero. not updated"))

				return
			}

			rateCh <- rate

			w.Write([]byte(fmt.Sprintf("rate updated: %d", rate)))
		})
		http.ListenAndServe(":3000", r)
	}(rateCh)

	wg.Add(1)
	go func(rateCh <-chan int, reqCh <-chan int, respCh chan<- int) {
		defer wg.Done()

		ticker := time.NewTicker(time.Second)

		rate := <-rateCh
		bucket := ratelimit.NewBucketWithRate(float64(rate), int64(rate))
		for {
			select {
			case rate := <-rateCh:
				fmt.Printf("rate: %d\n", rate)

				left := bucket.Available()

				bucket = ratelimit.NewBucketWithRate(float64(rate), int64(rate))
				bucket.Wait(left)
			case req := <-reqCh:
				bucket.Wait(int64(req))

				respCh <- req
			case <-ticker.C:
				fmt.Printf("Tick: rate: %.1f available %d\n", bucket.Rate(), bucket.Available())
			}
		}

	}(rateCh, reqCh, respCh)

	wg.Add(1)
	go func(reqCh chan int, respCh <-chan int) {
		defer wg.Done()

		connextra := amqpextra.New(
			func() (*amqp.Connection, error) {
				return amqp.Dial(amqpDsn)
			},
			nil,
			log.Printf,
			log.Printf,
		)

		connCh, closeCh := connextra.Get()
		publisher := amqpextra.NewPublisher(
			connCh,
			closeCh,
			nil,
			intiCh,
			log.Printf,
			log.Printf,
		)

		for {
			select {
			case reqCh <- 1:
				num := <-respCh

				for i := 0; i < num; i++ {
					err := <-publisher.Publish("", queue, false, false, amqp.Publishing{
						ContentType:   "application/json",
						CorrelationId: uuid.New().String(),
						ReplyTo:       replyQueue,
						MessageId:     uuid.New().String(),
						Body:          []byte(payload),
					})

					if err != nil {
						log.Printf("publish: %v", err)
					}
				}
			}
		}
	}(reqCh, respCh)

	wg.Wait()
}

func init() {
	flag.StringVar(&amqpDsn, "amqp", "", "amqp dsn, like amqp://guest:guest@localhost:5672//")
	flag.StringVar(&payload, "payload", "", "message payload")
	flag.StringVar(&queue, "queue", "", "publish message to a queue")
	flag.StringVar(&replyQueue, "reply-queue", "", "reply to queue")
	flag.IntVar(&rate, "rate", 20, "publish rate limit")
	flag.IntVar(&port, "port", 3000, "http server port (for change rate api)")

	flag.Parse()

	if amqpDsn == "" {
		fmt.Printf("-amqp flag is required")
		flag.Usage()

		os.Exit(1)
	}

	if port < 0 || port > 65000 {
		fmt.Printf("-port flag is required")
		flag.Usage()

		os.Exit(1)
	}

	if rate < 1 {
		fmt.Printf("-rate flag is required")
		flag.Usage()

		os.Exit(1)
	}

	if queue == "" {
		fmt.Printf("-queue flag is required")
		flag.Usage()

		os.Exit(1)
	}

	if replyQueue == "" {
		fmt.Printf("-reply-queue flag is required")
		flag.Usage()

		os.Exit(1)
	}

	if payload == "" {
		fmt.Printf("-payload flag is required")
		flag.Usage()

		os.Exit(1)
	}

	fmt.Println(amqpDsn)
}

func intiCh(conn *amqp.Connection) (*amqp.Channel, error) {
	return conn.Channel()
}
