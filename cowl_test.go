package cowl_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"

	"github.com/reedobrien/cowl"
)

func ShortFlushPeriod(w *cowl.Writer) { w.FlushPeriod = 7 * time.Millisecond }

func TestDefaultSettings(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s")
	equals(t, tut.FlushPeriod, cowl.DefaultFlushPeriod)
	equals(t, tut.FlushSize, cowl.DefaultFlushSize)
}

func TestOptionalSettings(t *testing.T) {
	fs := 100
	fp := time.Second
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s",
		func(w *cowl.Writer) { w.FlushSize = fs },
		func(w *cowl.Writer) { w.FlushPeriod = fp })
	equals(t, tut.FlushPeriod, fp)
	equals(t, tut.FlushSize, fs)

}

func TestMustNewWriterPanicsNilAPI(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustNewWriter failed to panic with nil API")
		}
	}()

	_ = cowl.MustNewWriter(nil, "testgroup", "teststream")
}

func TestMustNewWriterPanicsEmptyGroup(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustNewWriter failed to panic with nil API")
		}
	}()

	cowlAPI := &dummyCowlAPI{}
	_ = cowl.MustNewWriter(cowlAPI, "", "teststream")
}

func TestMustNewWriterPanicsEmptyStream(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustNewWriter failed to panic with nil API")
		}
	}()

	cowlAPI := &dummyCowlAPI{}
	_ = cowl.MustNewWriter(cowlAPI, "g", "")
}

func TestWriter(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s")
	got, err := tut.Write([]byte("foo"))
	ok(t, err)
	equals(t, got, 3)
}

func TestRejectedLogsError(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken: aws.String("1"),
		RejectedLogEventsInfo: &cloudwatchlogs.RejectedLogEventsInfo{
			ExpiredLogEventEndIndex: aws.Int64(1)},
	}}

	tut := cowl.MustNewWriter(api, "g", "s", ShortFlushPeriod)
	b, err := tut.Write([]byte("1\n2\n3\n"))
	ok(t, err)
	equals(t, b, 6)
	time.Sleep(10 * time.Millisecond)
	equals(t, len(api.plei), 3)
	equals(t, *api.plei[0].Message, "1")
	equals(t, *api.plei[1].Message, "2")
	equals(t, *api.plei[2].Message, "3")
}

func TestWriterWritesFlushPeriod(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s", ShortFlushPeriod)
	b, err := tut.Write([]byte("1\n2\n3\n"))
	ok(t, err)
	equals(t, b, 6)
	time.Sleep(10 * time.Millisecond)
	equals(t, len(api.plei), 3)
	equals(t, *api.plei[0].Message, "1")
	equals(t, *api.plei[1].Message, "2")
	equals(t, *api.plei[2].Message, "3")
}

func TestWriterWritesFlushSize(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s", func(w *cowl.Writer) { w.FlushSize = 2 })
	b, err := tut.Write([]byte("1\n2\n3\n"))
	ok(t, err)
	equals(t, b, 6)
	equals(t, len(api.plei), 2)
	equals(t, *api.plei[0].Message, "1")
	equals(t, *api.plei[1].Message, "2")
}

func TestWriterFlushesOnClose(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s",
		func(w *cowl.Writer) { w.FlushSize = 2 },
		ShortFlushPeriod)
	b, err := tut.Write([]byte("1\n2\n3\n"))
	ok(t, err)
	equals(t, b, 6)
	equals(t, len(api.plei), 2)
	equals(t, *api.plei[0].Message, "1")
	equals(t, *api.plei[1].Message, "2")
	tut.Close()
	time.Sleep(10 * time.Millisecond)
	equals(t, len(api.plei), 1)
	equals(t, *api.plei[0].Message, "3")
}

func TestWriterFlushesOnCloseAfterTimeout(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s",
		func(w *cowl.Writer) { w.FlushSize = 2 },
		ShortFlushPeriod)
	b, err := tut.Write([]byte("1\n2\n3\n"))
	ok(t, err)
	equals(t, b, 6)
	equals(t, len(api.plei), 2)
	equals(t, *api.plei[0].Message, "1")
	equals(t, *api.plei[1].Message, "2")
	tut.Close()
	time.Sleep(10 * time.Millisecond)
	equals(t, len(api.plei), 1)
	equals(t, *api.plei[0].Message, "3")
}

func TestWriterFlushesOnCancel(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	tut := cowl.MustNewWriterWithContext(ctx, api, "g", "s",
		func(w *cowl.Writer) { w.FlushSize = 2 },
		ShortFlushPeriod)
	b, err := tut.Write([]byte("1\n2\n3\n"))
	ok(t, err)
	equals(t, b, 6)
	equals(t, len(api.plei), 2)
	equals(t, *api.plei[0].Message, "1")
	equals(t, *api.plei[1].Message, "2")
	cancel()
	time.Sleep(10 * time.Millisecond)
	equals(t, len(api.plei), 1)
	equals(t, *api.plei[0].Message, "3")
}

func TestWriterCreatesStream(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s")
	equals(t, tut.LogGroup(), "g")
	equals(t, tut.LogStream(), "s")

}

func TestClosedWriterIOPipeError(t *testing.T) {
	api := &dummyCowlAPI{pleo: &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken:     aws.String("1"),
		RejectedLogEventsInfo: nil,
	}}

	tut := cowl.MustNewWriter(api, "g", "s")
	equals(t, tut.LogGroup(), "g")
	equals(t, tut.LogStream(), "s")
	tut.Close()
	n, err := tut.Write([]byte("error"))
	equals(t, n, 0)
	equals(t, err, io.ErrClosedPipe)

}

func TestMustNewWriterSucceedsExistingStream(t *testing.T) {
	var r interface{}
	defer func() {
		if r = recover(); r != nil {
			t.Errorf("MustNewWriter paniced but shouldn't have on failed stream creation")
		}
	}()

	api := &dummyCowlAPI{cerr: awserr.New(cloudwatchlogs.ErrCodeResourceAlreadyExistsException, "The specified resource already exists", nil)}
	_ = cowl.MustNewWriter(api, "g", "s")
}

func TestMustNewWriterPanicsFailedStream(t *testing.T) {
	var r interface{}
	defer func() {
		if r = recover(); r == nil {
			t.Errorf("MustNewWriter failed to panic on failed stream creation")
		}
		equals(t, r.(error).Error(), "InvalidOperationException: fake message")
	}()

	api := &dummyCowlAPI{cerr: awserr.New(cloudwatchlogs.ErrCodeInvalidOperationException, "fake message", nil)}
	_ = cowl.MustNewWriter(api, "g", "s")
}

func TestMustNewWriterPanicsOther(t *testing.T) {
	var r interface{}
	defer func() {
		if r = recover(); r == nil {
			t.Errorf("MustNewWriter failed to panic on failed stream creation")
		}
		equals(t, r.(error).Error(), "boom")
	}()

	api := &dummyCowlAPI{cerr: errors.New("boom")}
	_ = cowl.MustNewWriter(api, "g", "s")
}

type dummyCowlAPI struct {
	cerr error
	clsi *cloudwatchlogs.CreateLogStreamInput
	plei []*cloudwatchlogs.InputLogEvent
	pleo *cloudwatchlogs.PutLogEventsOutput
}

func (d *dummyCowlAPI) PutLogEvents(in *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error) {
	d.plei = in.LogEvents[:]
	return d.pleo, nil
}

func (d *dummyCowlAPI) CreateLogStream(clsi *cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	if d.cerr != nil {
		return nil, d.cerr
	}
	d.clsi = clsi
	return &cloudwatchlogs.CreateLogStreamOutput{}, nil
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if got is not equal to want.
func equals(tb testing.TB, got, want interface{}) {
	if !reflect.DeepEqual(got, want) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\tgot: %#v\n\n\twant: %#v\033[39m\n\n", filepath.Base(file), line, got, want)
		tb.FailNow()
	}
}

// assert fails the test if the condition is false.
// func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
// 	if !condition {
// 		_, file, line, _ := runtime.Caller(1)
// 		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
// 		tb.FailNow()
// 	}
// }
