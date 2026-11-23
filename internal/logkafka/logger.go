// Package logkafka appends one JSON record per completed request to Kafka for
// offline analysis/replay. With no brokers configured it is a no-op.
package logkafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Record struct {
	Timestamp    time.Time `json:"timestamp"`
	RequestID    string    `json:"request_id"`
	Tenant       string    `json:"tenant"`
	Model        string    `json:"model"`
	ServedModel  string    `json:"served_model"`
	Worker       string    `json:"worker"`
	Route        string    `json:"route"`
	Status       string    `json:"status"`
	OutputTokens int       `json:"output_tokens"`
	CachedTokens int64     `json:"cached_tokens"`
	TTFTMs       float64   `json:"ttft_ms"`
	TotalMs      float64   `json:"total_ms"`
}

type Logger struct{ writer *kafka.Writer }

func New(brokers []string, topic string) *Logger {
	if len(brokers) == 0 {
		return &Logger{}
	}
	return &Logger{writer: &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: topic,
		Balancer: &kafka.LeastBytes{}, Async: true,
		RequiredAcks: kafka.RequireOne,
	}}
}

func (l *Logger) Write(ctx context.Context, r Record) {
	if l == nil || l.writer == nil {
		return
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	if err := l.writer.WriteMessages(ctx, kafka.Message{Key: []byte(r.RequestID), Value: b}); err != nil {
		log.Printf("kafka log: %v", err)
	}
}

func (l *Logger) Close() {
	if l != nil && l.writer != nil {
		_ = l.writer.Close()
	}
}
