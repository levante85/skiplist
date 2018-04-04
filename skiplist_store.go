package skiplist

import (
	"fmt"
	"os"
	"syscall"
)

//Store the interface describes what a methods a backing store
// should implement in order to be accepted by the database engine
type Store interface {
	Create() error
	WriteAt(b []byte, off int64) (int, error)
	ReadAt(b []byte, off int64) (int, error)
	Sync(off int64, n int64)
	Close() error
}

//FILE_SIZE is the default size for each db file ~68Gb
const FILE_SIZE int64 = 4096 * 4096 * 4096

// FileStore is the dt responsible for backing the skiplist on disk
type FileStore struct {
	files            []*os.File
	data             [][]byte
	fname            string
	current          int64
	FileStoreMaxsize int64
}

func newFileStore() *FileStore {
	return &FileStore{
		fname:            "index.",
		FileStoreMaxsize: FILE_SIZE * 15,
	}
}

//Create new FileStore backing
func (s *FileStore) Create() error {
	if err := s.resize(0); err != nil {
		return err
	}

	return nil
}

//WriteAt write at said location
func (s *FileStore) WriteAt(b []byte, off int64) (int, error) {
	if err := s.resize(int64(len(b))); err != nil {
		return -1, err
	}

	if off >= s.current {
		s.current += off
	}

	return s.files[len(s.files)-1].WriteAt(b, off)
}

//ReadAt write at said location
func (s *FileStore) ReadAt(b []byte, off int64) (int, error) {
	return s.files[len(s.files)].ReadAt(b, off)
}

func (s *FileStore) resize(size int64) error {
	if size+s.current > FILE_SIZE || size == 0 {
		fname := fmt.Sprintf("%v%v", s.fname, len(s.files))
		file, err := os.Create(fname)
		if err != nil {
			return err
		}

		if err := file.Truncate(FILE_SIZE); err != nil {
			return err
		}

		s.files = append(s.files, file)
	}

	return nil
}

// Sync either sync the everything of calls sync file range with the
// specified off and n number of bytes ( sync only the pages the need
// to be synched
func (s *FileStore) Sync(off int64, n int64) {
	if off == 0 && n == 0 {
		syscall.Sync()
		return
	}
	syscall.SyncFileRange(int(s.files[len(s.files)-1].Fd()), off, n, 0)
}

// Close the FileStore and syncs
func (s *FileStore) Close() error {
	for i := range s.files {
		if err := s.files[i].Close(); err != nil {
			return err
		}
	}

	s.Sync(0, 0)

	return nil
}

// MappedStore is a memory mapped store that only maps for writes
type MappedStore struct {
	fstore *FileStore
	mstore [][]byte
}

func newMappedStore() *MappedStore {
	return &MappedStore{newFileStore(), nil}
}

//Create a new mapped store
func (m *MappedStore) Create() error {
	if err := m.fstore.Create(); err != nil {
		return err
	}

	prot := syscall.PROT_WRITE | syscall.PROT_READ
	flag := syscall.MAP_SHARED
	fd := m.fstore.files[len(m.fstore.files)-1].Fd()
	buf, err := syscall.Mmap(int(fd), 0, int(FILE_SIZE), prot, flag)
	if err != nil {
		return err
	}
	m.mstore = append(m.mstore, buf)

	return nil
}

//WriteAt write at said location
func (m *MappedStore) WriteAt(b []byte, off int64) (int, error) {
	if err := m.fstore.resize(int64(len(b))); err != nil {
		return -1, err
	}

	if off >= m.fstore.current {
		m.fstore.current += off
	}
	//TODO implement
	return -1, nil
}

//ReadAt write at said location
func (m *MappedStore) ReadAt(b []byte, off int64) (int, error) {
	//TODO implement

	return -1, nil
}

// Sync syncs the underline mapped storage or a region of it if anything
// other than zero is specified to it
func (m *MappedStore) Sync(off int64, n int64) error {
	return nil
}

// Close the FileStore and syncs
func (m *MappedStore) Close() error {
	for i := range m.fstore.files {
		if err := syscall.Munmap(m.mstore[i]); err != nil {
			return err
		}
		m.mstore[i] = nil

		if err := m.fstore.files[i].Close(); err != nil {
			return err
		}
	}

	m.Sync(0, 0)

	return nil
}
