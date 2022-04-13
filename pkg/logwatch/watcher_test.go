package logwatch

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ViaQ/logerr/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	t   *testing.T
	dir string
	w   *Watcher
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return &fixture{t: t, dir: dir}
}

func (f *fixture) path(logname string) string { return filepath.Join(f.dir, logname) }

// create or append to a log
func (f *fixture) log(path, data string) {
	f.t.Helper()
	require.NoError(f.t, os.MkdirAll(filepath.Dir(path), 0700))
	log, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(f.t, err)
	defer log.Close()
	_, err = log.Write([]byte(data))
	require.NoError(f.t, err)
}

func (f *fixture) watch() {
	f.t.Helper()
	var err error
	f.w, err = New(f.dir)
	require.NoError(f.t, err)
	go f.w.Watch()
	f.t.Cleanup(f.w.Close)
}

func (f *fixture) counter(path string) prometheus.Counter {
	f.t.Helper()
	var l LogLabels
	require.True(f.t, l.Parse(path), path)
	counter, err := f.w.metrics.GetMetricWithLabelValues(l.Namespace, l.Name, l.UUID, l.Container)
	require.NoError(f.t, err)
	return counter
}

func (f *fixture) count(path string) int {
	f.t.Helper()
	c := f.counter(path)
	m := &dto.Metric{}
	require.NoError(f.t, c.Write(m))
	return int(m.Counter.GetValue())
}

func (f *fixture) assertCounterReaches(path string, n int) {
	f.t.Helper()
	assert.Eventually(f.t, func() bool { return f.count(path) == n }, time.Second, time.Second/10,
		"want %v, got %v: %v", n, f.count(path), path)
}

func TestWatchesFiles(t *testing.T) {
	f := newFixture(t)
	hello, goodbye := "hello\n", "goodbye\n"

	// Create a log file before watch starts.
	before := f.path("before_loki-receiver_8cdbeb1b-f8bd-4c56-97d0-1d984060a846/loki-receiver/0.log")
	f.log(before, hello)
	f.watch()

	// Metric should be tracked
	for i := 1; i < 3; i++ {
		f.assertCounterReaches(before, i*len(hello))
		f.log(before, hello)
	}

	// Create a file after watch is running, metric should be tracked.
	after := f.path("after_loki-server_efc2acf0-387d-4274-975f-d5f77a4ffb3e/loki-server/1.log")
	for i := 1; i < 3; i++ {
		f.log(after, goodbye)
		f.assertCounterReaches(after, i*len(goodbye))
	}

	// Delete a log directory, should be removed from metrics (value goes to 0).
	assert.NoError(t, os.RemoveAll(filepath.Dir(filepath.Dir(before))))
	f.assertCounterReaches(before, 0)

	// Test rollover with multiple files, only collect increases
}

func TestWatchesMultiLogs(t *testing.T) {
	f := newFixture(t)
	hello, goodbye := "hello\n", "goodbye\n"

	some0 := f.path("somens_somepod_8cdbeb1b-f8bd-4c56-97d0-1d984060a846/somecontainer/0.log")
	some1 := f.path("somens_somepod_8cdbeb1b-f8bd-4c56-97d0-1d984060a846/somecontainer/1.log")
	before3 := f.path("somens_somepod_8cdbeb1b-f8bd-4c56-97d0-1d984060a846/beforecontainer/3.log")
	before4 := f.path("somens_somepod_8cdbeb1b-f8bd-4c56-97d0-1d984060a846/beforecontainer/4.log")

	f.log(before3, hello)

	f.watch()

	// Count existing files
	f.assertCounterReaches(before3, len(hello))
	// Count writes to new log file
	f.log(before4, goodbye)
	f.assertCounterReaches(before3, len(hello)+len(goodbye))

	// Count creation of multiple log files
	f.log(some0, hello)
	f.log(some1, goodbye)
	f.assertCounterReaches(some0, len(hello)+len(goodbye))

	// Count writes to all log files.
	f.log(some0, hello)
	f.assertCounterReaches(some0, 2*len(hello)+len(goodbye))
	f.log(some1, goodbye)
	f.assertCounterReaches(some0, 2*len(hello)+2*len(goodbye))

	// Delete individual log files, should not change the counter
	n := f.count(some0)
	assert.NoError(t, os.RemoveAll(filepath.Dir(some0)))
	assert.Equal(t, n, f.count(some0))
	assert.NoError(t, os.RemoveAll(filepath.Dir(some1)))
	assert.Equal(t, n, f.count(some0))

	// Re-create a log file, should add to counter
	f.log(some0, hello)
	f.assertCounterReaches(some0, n+len(hello))
}

func TestMain(m *testing.M) {
	log.SetLogLevel(3)
	m.Run()
}
