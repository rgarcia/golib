package sfxclient

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAsyncMultiTokenSinkStartup(t *testing.T) {
	Convey("A default sink", t, func() {
		s := NewAsyncMultiTokenSink()

		Convey("should be able to startup successfully", func() {
			So(s.Startup(int64(1), 30, IngestEndpointV2, EventIngestEndpointV2, DefaultUserAgent), ShouldBeNil)
		})

		Convey("should be able to startup successfully without a timebuffer", func() {
			So(s.Startup(int64(3), 30, "", "", ""), ShouldBeNil)
		})

		Convey("should not be able to startup if it's already running", func() {
			So(s.Startup(int64(1), 30, "", "", ""), ShouldBeNil)
			So(s.Startup(int64(1), 30, "", "", ""), ShouldNotBeNil)
		})
	})
}

func TestAddDataToAsyncMultitokenSink(t *testing.T) {
	Convey("A default sink", t, func() {
		s := NewAsyncMultiTokenSink()
		ctx := context.Background()
		dps := GoMetricsSource.Datapoints()
		evs := GoEventSource.Events()

		Convey("shouldn't accept dps and events with a context if a token isn't provided in the context", func() {
			So(errors.Details(s.AddEvents(ctx, evs)), ShouldContainSubstring, "no value was found on the context with key")
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "no value was found on the context with key")
		})

		Convey("shouldn't accept dps and events if the sink hasn't started", func() {
			ctx = context.WithValue(ctx, TokenCtxKey, "HELLOOOOOO")
			So(errors.Details(s.AddEvents(ctx, evs)), ShouldContainSubstring, "unable to add events: there was an error while hashing the token to a worker. no available workers")
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "unable to add datapoints: there was an error while hashing the token to a worker. no available workers")
			So(errors.Details(s.AddEventsWithToken("HELLOOOOO", evs)), ShouldContainSubstring, "unable to add events: there was an error while hashing the token to a worker. no available workers")
			So(errors.Details(s.AddDatapointsWithToken("HELLOOOOOO", dps)), ShouldContainSubstring, "unable to add datapoints: there was an error while hashing the token to a worker. no available workers")
		})

		Convey("shouldn't accept dps and events if the sink has started, but the workers have shutdown", func() {
			ctx = context.WithValue(ctx, TokenCtxKey, "HELLOOOOOO")
			s.ShutdownTimeout = time.Second * 1
			So(s.Startup(int64(2), 5000, "", "", ""), ShouldBeNil)
			So(s.Close(), ShouldBeNil)
			So(errors.Details(s.AddEvents(ctx, evs)), ShouldContainSubstring, "unable to add events: the worker has been stopped")
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "unable to add datapoints: the worker has been stopped")
			So(errors.Details(s.AddEventsWithToken("HELLOOOOO", evs)), ShouldContainSubstring, "unable to add events: the worker has been stopped")
			So(errors.Details(s.AddDatapointsWithToken("HELLOOOOOO", dps)), ShouldContainSubstring, "unable to add datapoints: the worker has been stopped")
		})
	})
}

func TestAsyncMultiTokenSinkClose(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should not be able to close if it's not running", func() {
			s := NewAsyncMultiTokenSink()
			s.ShutdownTimeout = time.Millisecond * 500
			So(s.Close(), ShouldNotBeNil)
			So(s.Startup(int64(1), 2, "", "", ""), ShouldBeNil)
			So(s.Close(), ShouldBeNil)
			So(errors.Details(s.Close()), ShouldContainSubstring, "unable to stop the sink because it has already stopped")
		})

		Convey("should be able to close successfully when no data has been added to it", func() {
			s := NewAsyncMultiTokenSink()
			So(s.Startup(int64(2), 25, "", "", ""), ShouldBeNil)
			s.ShutdownTimeout = time.Millisecond * 500
			So(s.Close(), ShouldBeNil)
		})
	})
}

func TestAsyncMultiTokenSinkWorkerWithDatapoints(t *testing.T) {
	Convey("An AsyncMultiTokenSink Worker", t, func() {
		Convey("should asynchronously close and timeout while datapoints are being emitted", func() {
			s := NewAsyncMultiTokenSink()
			dps := GoMetricsSource.Datapoints()
			shutdownTimeout := time.Second * 1
			So(s.Startup(int64(1), 5, "", "", ""), ShouldBeNil)
			s.dpWorkers[0].errorHandler = func(e error) error {
				time.Sleep(time.Second * 3)
				return DefaultErrorHandler(e)
			}
			go func() {
				for i := 0; i < 500000; i++ {
					_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
				}
			}()
			time.Sleep(500 * time.Millisecond) // wait for stuff to get to sink
			So(s.dpWorkers[0].Stop(shutdownTimeout), ShouldBeFalse)
		})
		Convey("should stop and return true if it processes all pending datapoints before the timeout", func() {
			s := NewAsyncMultiTokenSink()
			dps := GoMetricsSource.Datapoints()
			shutdownTimeout := time.Second * 3

			So(s.Startup(int64(1), 7, "", "", ""), ShouldBeNil)
			for i := 0; i < 5; i++ {
				_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
			}
			So(s.dpWorkers[0].Stop(shutdownTimeout), ShouldBeTrue)
		})
	})
}

func TestAsyncMultiTokenSinkWorkerWithEvents(t *testing.T) {
	Convey("An AsyncMultiTokenSink Worker", t, func() {
		Convey("should asynchronously close and timeout while events are being emitted", func() {
			s := NewAsyncMultiTokenSink()
			evs := GoEventSource.Events()
			shutdownTimeout := time.Second * 0

			So(s.Startup(int64(1), 5, "", "", ""), ShouldBeNil)
			go func() {
				for i := 0; i < 500000; i++ {
					_ = s.AddEventsWithToken("HELLOOOOOO", evs)
				}
			}()
			time.Sleep(500 * time.Millisecond) // wait for stuff to get to the sink
			So(s.evWorkers[0].Stop(shutdownTimeout), ShouldBeFalse)
		})
		Convey("should stop and return true if it processes all pending events before the timeout", func() {
			s := NewAsyncMultiTokenSink()
			evs := GoEventSource.Events()
			shutdownTimeout := time.Second * 3

			So(s.Startup(int64(1), 7, "", "", ""), ShouldBeNil)
			for i := 0; i < 5; i++ {
				_ = s.AddEventsWithToken("HELLOOOOOO", evs)
			}
			So(s.evWorkers[0].Stop(shutdownTimeout), ShouldBeTrue)
		})
	})
}

func TestAsyncMultiTokenSinkShutdownDroppedDatapoints(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should raise an error if it's possible that datapoints were dropped", func() {
			s := NewAsyncMultiTokenSink()
			dps := GoMetricsSource.Datapoints()
			s.ShutdownTimeout = (time.Second * 0)
			// increase the number of datapoints added to the sink in a single call
			for i := 0; i < 3; i++ {
				dps = append(dps, GoMetricsSource.Datapoints()...)
			}
			// intentionally slow down emission to test shutdown timeout
			s.errorHandler = func(e error) error {
				time.Sleep(3 * time.Second)
				return DefaultErrorHandler(e)
			}
			So(s.Startup(int64(1), 25, "", "", ""), ShouldBeNil)
			for i := 0; i < 5; i++ {
				go func() {
					for i := 0; i < 500000; i++ {
						_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
					}
				}()
			}
			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(errors.Details(s.Close()), ShouldContainSubstring, "may have been dropped")
		})
	})
}

func TestAsyncMultiTokenSinkShutdownDroppedEvents(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should raise an error if it's possible that events were dropped", func() {
			s := NewAsyncMultiTokenSink()
			evs := GoEventSource.Events()
			s.ShutdownTimeout = (time.Second * 0)
			// increase the number of events added to the sink in a single call
			for i := 0; i < 3; i++ {
				evs = append(evs, GoEventSource.Events()...)
			}
			// intentionally slow down emission to test shutdown timeout
			s.errorHandler = func(e error) error {
				time.Sleep(3 * time.Second)
				return DefaultErrorHandler(e)
			}
			So(s.Startup(int64(1), 25, "", "", ""), ShouldBeNil)
			for i := 0; i < 5; i++ {
				go func() {
					for i := 0; i < 500000; i++ {
						_ = s.AddEventsWithToken("HELLOOOOOO", evs)
					}
				}()
			}
			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(errors.Details(s.Close()), ShouldContainSubstring, "may have been dropped")
		})
	})
}

func TestAsyncMultiTokenSinkCleanClose(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should gracefully shutdown after some data is added to it", func() {
			s := NewAsyncMultiTokenSink()
			dps := GoMetricsSource.Datapoints()
			evs := GoEventSource.Events()
			s.ShutdownTimeout = (time.Second * 5)

			So(s.Startup(int64(2), 2500, "", "", ""), ShouldBeNil)

			go func() {
				for i := 0; i < 5; i++ {
					_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
					_ = s.AddDatapointsWithToken("HELLOOOOOO2", dps)
				}
			}()

			go func() {
				for i := 0; i < 5; i++ {
					_ = s.AddEventsWithToken("HELLOOOOOO", evs)
					_ = s.AddEventsWithToken("HELLOOOOOO2", evs)
				}
			}()

			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(s.Close(), ShouldBeNil)
		})
	})
}

func TestAsyncMultiTokenSinkHasherError(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		s := NewAsyncMultiTokenSink()
		dps := GoMetricsSource.Datapoints()
		evs := GoEventSource.Events()

		Convey("should not be able to add datapoints or events if the hasher is nil", func() {
			s.Hasher = nil
			So(s.Startup(int64(3), 30, "", "", ""), ShouldBeNil)
			So(s.AddDatapointsWithToken("HELLOOOOOO", dps), ShouldNotBeNil)
			So(s.AddEventsWithToken("HELLOOOOOO", evs), ShouldNotBeNil)
		})
	})
}

func BenchmarkAsyncMultiTokenSinkCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewAsyncMultiTokenSink()
	}
}

// Without TimeBuffer
func BenchmarkAsyncMultiTokenSinkAddIndividualDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	l := len(points)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			var dp = make([]*datapoint.Datapoint, 0)
			dp = append(dp, points[j])
			_ = sink.AddDatapoints(ctx, dp)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkAddSeveralDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddDatapoints(ctx, points)
	}
}

func BenchmarkAsyncMultiTokenSinkAddIndividualEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	l := len(events)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			var ev = make([]*event.Event, 0)
			ev = append(ev, events[j])
			_ = sink.AddEvents(ctx, ev)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkAddSeveralEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddEvents(ctx, events)
	}
}

// With TimeBuffer
func BenchmarkAsyncMultiTokenSinkWithBufferAddIndividualDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	l := len(points)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			var dp = make([]*datapoint.Datapoint, 0)
			dp = append(dp, points[j])
			_ = sink.AddDatapoints(ctx, dp)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkWithBufferAddSeveralDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddDatapoints(ctx, points)
	}
}

func BenchmarkAsyncMultiTokenSinkWithBufferAddIndividualEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	l := len(events)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			var ev = make([]*event.Event, 0)
			ev = append(ev, events[j])
			_ = sink.AddEvents(ctx, ev)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkWithBufferAddSeveralEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink()
	_ = sink.Startup(int64(1), 30, "", "", "")
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddEvents(ctx, events)
	}
}
