// Package cowl implements an io.Writer that sends logs in batches to
// Cloudwatch Logs.
// Copyright © 2017 Reed O'Brien <reed+oss@reedobrien.com>.
// All rights reserved. Use of this source code is governed by a
// MIT-style license that can be found in the LICENSE file.
package cowl

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

const (
	// DefaultFlushPeriod is the default period between flushes if the max size
	// threshold isn't met.
	DefaultFlushPeriod = 10 * time.Second
	// DefaultFlushSize is the maximum size the buffer can reach before
	// flushing if the flush period duration hasn't passed.
	DefaultFlushSize = 1000
)

// ClientAPI is an interface limited to the methods we want to use.
// https://github.com/aws/aws-sdk-go/blob/master/service/cloudwatchlogs/cloudwatchlogsiface/interface.go
type ClientAPI interface {
	PutLogEvents(*cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
	CreateLogStream(*cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error)
}

// MustNewWriter returns a new Writer for use or panics.
func MustNewWriter(api ClientAPI, group, stream string, options ...func(*Writer)) *Writer {
	return MustNewWriterWithContext(context.Background(), api, group, stream, options...)

}

// MustNewWriterWithContext returns a Writer using the provided context.
func MustNewWriterWithContext(ctx context.Context, api ClientAPI, group, stream string, options ...func(*Writer)) *Writer {

	if api == nil {
		panic("a client API implementation must be provided")
	}
	if group == "" || stream == "" {
		panic("group and stream must be set")
	}

	events := make(chan *cloudwatchlogs.InputLogEvent, 1)
	ctx, cancel := context.WithCancel(ctx)

	w := &Writer{
		API: api,

		FlushSize:   DefaultFlushSize,
		FlushPeriod: DefaultFlushPeriod,

		cancel:  cancel,
		ctx:     ctx,
		events:  events,
		entries: &buffer{},
		group:   aws.String(group),
		stream:  aws.String(stream),
	}

	_, err := api.CreateLogStream(
		&cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  w.group,
			LogStreamName: w.stream},
	)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
				panic(err)
			}
		} else {
			panic(err)
		}
	}

	for _, opt := range options {
		opt(w)
	}

	go w.run()

	return w
}

type buffer struct {
	sync.RWMutex
	q []*cloudwatchlogs.InputLogEvent
}

// Append appends the Event to q.
func (b *buffer) Append(e *cloudwatchlogs.InputLogEvent) {
	b.Lock()
	defer b.Unlock()
	b.q = append(b.q, e)
}

// Len return the length of the buffer.
func (b *buffer) Len() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.q)
}

// Empty returns the elements in q and empties q.
func (b *buffer) Empty() []*cloudwatchlogs.InputLogEvent {
	b.Lock()
	defer b.Unlock()
	slice := b.q[:]
	b.q = nil
	return slice
}

// Writer implements io.Writer and sends the incoming stream to CloudWatch Logs
// in batches.
type Writer struct {
	API         ClientAPI
	FlushPeriod time.Duration
	FlushSize   int

	cancel context.CancelFunc
	ctx    context.Context
	err    error

	mu                   sync.RWMutex
	closed               bool
	group, stream, token *string
	events               chan *cloudwatchlogs.InputLogEvent
	entries              *buffer
}

// Close closes the channel that accepts events. Signaling it is time to flush
// and exit.
func (w *Writer) Close() {
	close(w.events)
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
}

// Closed returns if the writer has already been closed.
func (w *Writer) Closed() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.closed
}

// Write implements io.Writer.
func (w *Writer) Write(b []byte) (int, error) {
	if w.Closed() {
		return 0, io.ErrClosedPipe
	}
	if w.err != nil {
		return 0, w.err
	}

	r := bufio.NewReader(bytes.NewReader(b))
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		w.events <- &cloudwatchlogs.InputLogEvent{
			Message: aws.String(scanner.Text()),
			// Timestamp for AWS is milliseconds since last epoch. See:
			// https://github.com/aws/aws-sdk-go/blob/master/service/cloudwatchlogs/api.go#L5813
			// I don't see any general docs about epoch timestamps, but this is
			// littered throughout AWS docs.
			Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
	}
	if err := scanner.Err(); err != nil {
		// Although it is possible we've read some lines in, we return 0
		// because if there is an error the caller should not trust the non
		// error return value.
		return 0, err
	}

	return len(b), nil
}

func (w *Writer) run() {
	defer w.cancel()
	for {
		select {
		case e, ok := <-w.events:
			if ok {
				w.entries.Append(e)
				if w.entries.Len() >= w.FlushSize {
					w.flush()
				}
			} else {
				// w.closed = true
				w.flush()
				return
			}
		case <-w.ctx.Done():
			if !w.Closed() {
				w.Close()
			}
		case <-time.After(w.FlushPeriod):
			if w.entries.Len() > 0 {
				w.flush()
			}
		}
	}

}

func (w *Writer) flush() {
	resp, err := w.API.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     w.entries.Empty(),
		LogGroupName:  w.group,
		LogStreamName: w.stream,
		SequenceToken: w.token,
	})

	if err != nil {
		w.err = err
	}

	if resp.RejectedLogEventsInfo != nil {
		w.err = awserr.New("RejectedLogEventsError", resp.RejectedLogEventsInfo.String(), nil)
	}

	w.token = resp.NextSequenceToken
}

// LogGroup returns the log group this writer writes to.
func (w *Writer) LogGroup() string {
	return *w.group
}

// LogStream returns the  log stream this writer writes to.
func (w *Writer) LogStream() string {
	return *w.stream
}
