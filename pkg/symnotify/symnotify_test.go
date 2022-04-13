package symnotify_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ViaQ/logerr/log"
	"github.com/log-file-metric-exporter/pkg/symnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var Join = filepath.Join

type Fixture struct {
	T                   *testing.T
	Root, Logs, Targets string
	Watcher             *symnotify.Watcher
}

func NewFixture(t *testing.T) *Fixture {
	t.Helper()
	f := &Fixture{T: t}

	var err error
	f.Root = t.TempDir()
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(f.Root) })

	f.Logs = Join(f.Root, "logs")
	f.Targets = Join(f.Root, "targets")
	for _, dir := range []string{f.Logs, f.Targets} {
		require.NoError(t, os.Mkdir(dir, os.ModePerm))
	}
	f.Watcher, err = symnotify.NewWatcher()
	require.NoError(t, err)
	t.Cleanup(func() { f.Watcher.Close() })
	return f
}

func (f *Fixture) Create(name string) (string, *os.File) {
	f.T.Helper()
	file, err := os.Create(name)
	require.NoError(f.T, err)
	f.T.Cleanup(func() { _ = file.Close() })
	return name, file
}

func (f *Fixture) Mkdir(name string) {
	f.T.Helper()
	err := os.Mkdir(name, 0777)
	require.NoError(f.T, err)
}

func (f *Fixture) Link(name string) (string, *os.File) {
	f.T.Helper()
	target, file := f.Create(Join(f.Targets, name))
	link := Join(f.Logs, name)
	require.NoError(f.T, os.Symlink(target, link))
	return link, file
}

func (f *Fixture) Event() symnotify.Event {
	f.T.Helper()
	e, err := f.Watcher.Event()
	require.NoError(f.T, err)
	return e
}

func TestCreateWriteRemove(t *testing.T) {
	f := NewFixture(t)
	assert, require := assert.New(t), require.New(t)
	// Create file before starting watcher
	log1, f1 := f.Create(Join(f.Logs, "log1"))
	require.NoError(f.Watcher.Add(f.Logs))
	// Create log after starting watcher
	log2, _ := f.Create(Join(f.Logs, "log2"))
	assert.Equal(f.Event(), symnotify.Event{Name: log2, Op: symnotify.Create})

	_, err := f1.Write([]byte("hello\n"))
	assert.NoError(err)
	assert.Equal(f.Event(), symnotify.Event{Name: log1, Op: symnotify.Write})

	assert.NoError(os.Remove(log1))
	assert.Equal(f.Event(), symnotify.Event{Name: log1, Op: symnotify.Remove})
	assert.NoError(os.Remove(log2))
	assert.Equal(f.Event(), symnotify.Event{Name: log2, Op: symnotify.Remove})
}

func TestWatchesRealFiles(t *testing.T) {
	f := NewFixture(t)
	assert, require := assert.New(t), require.New(t)

	// Create file before starting watcher
	log1, file1 := f.Create(Join(f.Logs, "log1"))
	require.NoError(f.Watcher.Add(f.Logs))
	// Create log after starting watcher
	log2, file2 := f.Create(Join(f.Logs, "log2"))
	assert.Equal(f.Event(), symnotify.Event{Name: log2, Op: symnotify.Create})

	// Write to real logs, check Events.
	nw, errw := file1.Write([]byte("hello1"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: log1, Op: symnotify.Write})
	}
	errt := file1.Truncate(0)
	if errt == nil {
		assert.Equal(f.Event(), symnotify.Event{Name: log1, Op: symnotify.Write})
	}
	nw, errw = file2.Write([]byte("hello2"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: log2, Op: symnotify.Write})
	}

	// Delete and rename real files
	newlog1 := Join(f.Logs, "newlog1")
	assert.NoError(os.Rename(log1, newlog1))
	assert.Equal(f.Event(), symnotify.Event{Name: log1, Op: symnotify.Rename})
	assert.Equal(f.Event(), symnotify.Event{Name: newlog1, Op: symnotify.Create})

	assert.NoError(os.Remove(log2))
	assert.Equal(f.Event(), symnotify.Event{Name: log2, Op: symnotify.Remove})

	nw, errw = file1.Write([]byte("x"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: newlog1, Op: symnotify.Write})
	}
}

func TestWatchesSymlinks(t *testing.T) {
	f := NewFixture(t)
	assert, require := assert.New(t), require.New(t)
	// Create link before starting watcher
	link1, file1 := f.Link("log1")
	require.NoError(f.Watcher.Add(f.Logs))
	link2, file2 := f.Link("log2")
	assert.Equal(f.Event(), symnotify.Event{Name: link2, Op: symnotify.Create})

	// Write to files, check Events on links.
	nw1, errw1 := file1.Write([]byte("hello"))
	if errw1 == nil && nw1 > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: link1, Op: symnotify.Write})
	}
	errt := file1.Truncate(0)
	if errt == nil {
		assert.Equal(f.Event(), symnotify.Event{Name: link1, Op: symnotify.Write})
	}
	nw2, errw2 := file2.Write([]byte("hello"))
	if errw2 == nil && nw2 > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: link2, Op: symnotify.Write})
	}
	errch := file2.Chmod(0444)
	if errch == nil {
		assert.Equal(f.Event(), symnotify.Event{Name: link2, Op: symnotify.Chmod})
	}

	// Rename and remove symlinks
	newlink1 := Join(f.Logs, "newlog1")
	assert.NoError(os.Rename(link1, newlink1))
	assert.Equal(f.Event(), symnotify.Event{Name: link1, Op: symnotify.Rename})
	assert.Equal(f.Event(), symnotify.Event{Name: newlink1, Op: symnotify.Create})

	assert.NoError(os.Remove(link2))
	assert.Equal(f.Event(), symnotify.Event{Name: link2, Op: symnotify.Remove})

	nw3, errw3 := file1.Write([]byte("x"))
	if errw3 == nil && nw3 > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: newlink1, Op: symnotify.Write})
	}
}

func TestWatchesSymlinkTargetsChanged(t *testing.T) {
	f := NewFixture(t)
	assert, require := assert.New(t), require.New(t)
	require.NoError(f.Watcher.Add(f.Logs))
	link, _ := f.Link("log")
	assert.Equal(f.Event(), symnotify.Event{Name: link, Op: symnotify.Create})

	// Replace link target with a new file.
	target := Join(f.Targets, "log")
	tempname, tempfile := f.Create(Join(f.Targets, "temp"))
	assert.NoError(os.Rename(tempname, target))
	assert.Equal(f.Event(), symnotify.Event{Name: link, Op: symnotify.Chmod})
	nw, errw := tempfile.Write([]byte("temp"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: link, Op: symnotify.Write})
	}
	got, err := ioutil.ReadFile((link))
	assert.NoError(err)
	assert.Equal(string(got), "temp")
}

func TestCreateRemoveEmpty(t *testing.T) {
	f := NewFixture(t)
	assert, require := assert.New(t), require.New(t)
	require.NoError(f.Watcher.Add(f.Logs))
	name, file := f.Create(Join(f.Logs, "foo"))
	_, err := file.Write([]byte("x"))
	assert.Equal(f.Event(), symnotify.Event{Name: name, Op: symnotify.Create})
	assert.Equal(f.Event(), symnotify.Event{Name: name, Op: symnotify.Write})
	file.Close()
	file, err = os.Open(name)
	assert.NoError(err)
	file.Close()
	require.NoError(os.Remove(name))
	file, err = os.Open(name)
	require.Error(err)
	assert.Equal(f.Event(), symnotify.Event{Name: name, Op: symnotify.Remove})
}

func TestWatchesSubdirectories(t *testing.T) {
	f := NewFixture(t)
	assert, require := assert.New(t), require.New(t)

	// Create file in subdir before starting watcher
	f.Mkdir(Join(f.Logs, "dir1"))
	log1, file1 := f.Create(Join(f.Logs, "dir1", "log1"))
	log2, file2 := f.Create(Join(f.Logs, "dir1", "log2"))
	f.Mkdir(Join(f.Logs, "dir1", "dir2"))
	log3, file3 := f.Create(Join(f.Logs, "dir1", "dir2", "log3"))
	require.NoError(f.Watcher.Add(f.Logs))

	// Create log after starting watcher
	log4, file4 := f.Create(Join(f.Logs, "dir1", "dir2", "log4"))
	assert.Equal(f.Event(), symnotify.Event{Name: log4, Op: symnotify.Create})

	// Write to logs, check Events.
	nw, errw := file1.Write([]byte("hello1"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: log1, Op: symnotify.Write})
	}
	errt := file1.Truncate(0)
	if errt == nil {
		assert.Equal(f.Event(), symnotify.Event{Name: log1, Op: symnotify.Write})
	}
	nw, errw = file2.Write([]byte("hello2"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: log2, Op: symnotify.Write})
	}
	nw, errw = file3.Write([]byte("hello3"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: log3, Op: symnotify.Write})
	}

	nw, errw = file4.Write([]byte("hello4"))
	if errw == nil && nw > 0 {
		assert.Equal(f.Event(), symnotify.Event{Name: log4, Op: symnotify.Write})
	}
}

func TestMain(m *testing.M) {
	log.SetLogLevel(3)
	m.Run()
}
