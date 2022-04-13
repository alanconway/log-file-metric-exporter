// Package logwatch watches Pod log files and updates metrics.
package logwatch

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/ViaQ/logerr/log"
	"github.com/fsnotify/fsnotify"
	"github.com/log-file-metric-exporter/pkg/symnotify"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	logFile   = regexp.MustCompile(`/([a-z0-9-]+)_([a-z0-9-]+)_([a-f0-9-]+)/([a-z0-9-]+)/.*\.log`)
	logPodDir = regexp.MustCompile(`/([a-z0-9-]+)_([a-z0-9-]+)_([a-f0-9-]+)$`)
)

// LogLabels are the labels for a Pod log file.
//
// NOTE: The log Path is not a label because it includes a variable "n.log" part that changes
// over the life of the same container.
type LogLabels struct {
	Namespace, Name, UUID, Container string
}

func (l *LogLabels) Parse(path string) (ok bool) {
	match := logFile.FindStringSubmatch(path)
	if match != nil {
		l.Namespace, l.Name, l.UUID, l.Container = match[1], match[2], match[3], match[4]
		return true
	}
	return false
}

type Watcher struct {
	watcher *symnotify.Watcher
	metrics *prometheus.CounterVec
	sizes   map[string]float64
}

func New(dir string) (*Watcher, error) {
	//Get new watcher
	watcher, err := symnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("error creating watcher: %w", err)
	}
	w := &Watcher{
		watcher: watcher,
		metrics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "log_logged_bytes_total",
			Help: "Total number of bytes written to a single log file path, accounting for rotations",
		}, []string{"namespace", "podname", "poduuid", "containername"}),
		sizes: make(map[string]float64),
	}
	if err := prometheus.Register(w.metrics); err != nil {
		return nil, err
	}
	if err := w.watcher.Add(dir); err != nil {
		return nil, err
	}
	update := func(path string, info os.FileInfo, err error) error { w.Update(path); return nil }
	if err := filepath.Walk(dir, update); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Watcher) Close() {
	w.watcher.Close()
	prometheus.Unregister(w.metrics)
}

func (w *Watcher) Update(path string) {
	var err error
	defer func() {
		if err != nil && !os.IsNotExist(err) {
			log.Error(err, "updating metric", "path", path)
		}
	}()

	var l LogLabels
	if l.Parse(path) { // Update metric for a log file
		var stat os.FileInfo
		stat, err = os.Stat(path)
		if err != nil {
			return
		}
		counter, err := w.metrics.GetMetricWithLabelValues(l.Namespace, l.Name, l.UUID, l.Container)
		if err != nil {
			return
		}
		lastSize, size := w.sizes[path], float64(stat.Size())
		w.sizes[path] = size
		var add float64
		if size >= lastSize {
			// File is static or has grown, add the difference to the counter.
			add = size - lastSize
		} else {
			// File has been truncated, treat like a new file.
			add = size
		}
		counter.Add(add)
		log.V(3).Info("updated metric", "path", path, "size", size)
		return
	}
	if infos, err := ioutil.ReadDir(path); err == nil { // Scan directories
		for _, info := range infos {
			w.Update(filepath.Join(path, info.Name()))
		}
	}
}

func (w *Watcher) Remove(path string) {
	if logPodDir.FindStringSubmatch(path) != nil { // This is a pod log directory
		for k, _ := range w.sizes { // Remove all counters for containers under this pod dir.
			if filepath.HasPrefix(k, path) {
				delete(w.sizes, k)
				var l LogLabels
				if l.Parse(k) {
					_ = w.metrics.DeleteLabelValues(l.Namespace, l.Name, l.UUID, l.Container)
				}
			}
		}
	}
}

func (w *Watcher) Watch() error {
	for {
		e, err := w.watcher.Event()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		case e.Op == fsnotify.Remove:
			w.Remove(e.Name)
		default:
			w.Update(e.Name)
		}
	}
}
