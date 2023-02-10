package hashfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/spf13/afero"
	"github.com/tidwall/buntdb"
)

type HashFS struct {
	db     *buntdb.DB
	blobFs afero.Fs
}

var errNotSupported = errors.New("not supported")

func NewHashFS(dbPath string, blobFs afero.Fs) (hfs *HashFS, err error) {
	db, err := buntdb.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer closeOnError(db, &err)
	db.SetConfig(buntdb.Config{SyncPolicy: buntdb.Always})
	return &HashFS{db: db, blobFs: blobFs}, nil
}

func (h *HashFS) fileKey(name string) string {
	return fmt.Sprintf("file:%s//%s", path.Dir(name), path.Base(name))
}

func (h *HashFS) fileInfo(tx *buntdb.Tx, name string) *hashFileInfo {
	if val, err := tx.Get(name); err == nil {
		hs := &hashFileInfo{}
		json.Unmarshal([]byte(val), hs)
		return hs
	}
	return nil
}

// Create creates a file in the filesystem, returning the file and an
// error, if any happens.
func (h *HashFS) Create(name string) (afero.File, error) {
	return nil, errNotSupported
}

// Mkdir creates a directory in the filesystem, return an error if any
// happens.
func (h *HashFS) Mkdir(name string, perm os.FileMode) error {
	for dir := name; dir != "."; dir = path.Dir(dir) {
		if err := h.db.Update(func(tx *buntdb.Tx) error {
			hs := h.fileInfo(tx, dir)
			if hs != nil && !hs.IsDir() {
				return &fs.PathError{Op: "mkdir", Path: name, Err: afero.ErrFileExists}
			}
		}); err != nil {
			return err
		}
	}
}

// MkdirAll creates a directory path and all parents that does not exist
// yet.
func (h *HashFS) MkdirAll(path string, perm os.FileMode) error {
	panic("not implemented") // TODO: Implement
}

// Open opens a file, returning it or an error, if any happens.
func (h *HashFS) Open(name string) (afero.File, error) {
	panic("not implemented") // TODO: Implement
}

// OpenFile opens a file using the given flags and the given mode.
func (h *HashFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	panic("not implemented") // TODO: Implement
}

// Remove removes a file identified by name, returning an error, if any
// happens.
func (h *HashFS) Remove(name string) error {
	panic("not implemented") // TODO: Implement
}

// RemoveAll removes a directory path and any children it contains. It
// does not fail if the path does not exist (return nil).
func (h *HashFS) RemoveAll(path string) error {
	panic("not implemented") // TODO: Implement
}

// Rename renames a file.
func (h *HashFS) Rename(oldname string, newname string) error {
	panic("not implemented") // TODO: Implement
}

// Stat returns a FileInfo describing the named file, or an error, if any
// happens.
func (h *HashFS) Stat(name string) (os.FileInfo, error) {
	panic("not implemented") // TODO: Implement
}

// The name of this FileSystem
func (h *HashFS) Name() string {
	panic("not implemented") // TODO: Implement
}

// Chmod changes the mode of the named file to mode.
func (h *HashFS) Chmod(name string, mode os.FileMode) error {
	panic("not implemented") // TODO: Implement
}

// Chown changes the uid and gid of the named file.
func (h *HashFS) Chown(name string, uid int, gid int) error {
	panic("not implemented") // TODO: Implement
}

// Chtimes changes the access and modification times of the named file
func (h *HashFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	panic("not implemented") // TODO: Implement
}

type hashFileInfo struct {
	info struct {
		Name    string      `json:"name"`
		Size    int64       `json:"size"`
		Hash    string      `json:"hash"`
		Mode    fs.FileMode `json:"mode"`
		ModTime time.Time   `json:"mtime"`
	}
}

func (hs *hashFileInfo) Name() string       { return hs.info.Name }
func (hs *hashFileInfo) Size() int64        { return hs.info.Size }
func (hs *hashFileInfo) Mode() fs.FileMode  { return hs.info.FileMode }
func (hs *hashFileInfo) ModTime() time.Time { return hs.info.ModTime }
func (hs *hashFileInfo) IsDir() bool        { return (hs.info.Mode & fs.ModeDir) != 0 }
func (hs *hashFileInfo) Sys() any           { return nil }
