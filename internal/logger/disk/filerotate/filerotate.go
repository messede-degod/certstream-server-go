package filerotate

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RotatableFile struct {
	sync.Mutex

	Directory string // file dir
	Name      string // file name
	Path      string // file path

	creationTime time.Time

	file       *os.File
	rotateType RotateType
	OnRotate   func(oldFilePath, oldFileName, newFilePath, newFileName string)
}

type RotateType string

const (
	ROTATE_HOURLY RotateType = "ROTATE_HOURLY"
	ROTATE_DAILY  RotateType = "ROTATE_DAILY"
)

func New(directory string, rotateType RotateType, onRotate func(oldFilePath, oldFileName, newFilePath, newFileName string)) (*RotatableFile, error) {
	file, _, filePath, ferr := newFile(directory, rotateType)
	if ferr != nil {
		return &RotatableFile{}, ferr
	}

	rotatableFile := RotatableFile{
		creationTime: time.Now().UTC(),
		Directory:    directory,
		Path:         filePath,
		file:         file,
		rotateType:   rotateType,
		OnRotate:     onRotate,
	}

	go rotatableFile.RotateFile()

	return &rotatableFile, nil
}

func newFile(directory string, rotateType RotateType) (file *os.File, newFileName string, filePath string, Error error) {
	now := time.Now().UTC()
	fileName := ""

	switch rotateType {
	case ROTATE_HOURLY:
		fileName = now.Format(time.RFC3339)
	case ROTATE_DAILY:
		fallthrough
	default:
		fileName = now.Format("2006-01-02")
	}

	filePath = filepath.Join(directory, fmt.Sprintf("%s.txt", fileName))

	derr := os.MkdirAll(directory, 0755)
	if derr != nil {
		return nil, "", filePath, derr
	}

	file, ferr := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if ferr != nil {
		return nil, "", filePath, ferr
	}

	return file, fileName, filePath, nil
}

func (rotatableFile *RotatableFile) RotateFile() {
	for {
		rotateAt := rotatableFile.getNextRotation()

		log.Printf("Next Disk log rotation is in: %f Hours\n", rotateAt.Hours())

		<-time.After(rotateAt) // wait untill duration

		newFile, newFileName, newFilePath, nfErr := newFile(rotatableFile.Directory, rotatableFile.rotateType)
		if nfErr != nil {
			log.Panicln("Error while creation new file for rotation: ", nfErr)
		}

		rotatableFile.Mutex.Lock()

		// switch to new file
		oldFile := rotatableFile.file
		oldFileName := rotatableFile.Name
		oldFilePath := rotatableFile.Path

		rotatableFile.file = newFile
		rotatableFile.Name = newFileName
		rotatableFile.Path = newFilePath

		rotatableFile.Mutex.Unlock()

		// sync and close old file
		oldFile.Sync()
		oldFile.Close()

		if rotatableFile.OnRotate != nil {
			go rotatableFile.OnRotate(oldFilePath, oldFileName, newFilePath, newFileName)
		}
	}
}

func (rotatableFile *RotatableFile) getNextRotation() time.Duration {
	now := time.Now().UTC()
	switch rotatableFile.rotateType {
	case ROTATE_HOURLY:
		return now.Add(time.Hour).Sub(now)
	case ROTATE_DAILY:
		fallthrough
	default:
		return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC).Sub(now) // time between now and midnight
	}
}

func (rotatableFile *RotatableFile) Write(b []byte) (n int, err error) {
	rotatableFile.Mutex.Lock()
	defer rotatableFile.Mutex.Unlock()
	return rotatableFile.file.Write(b)
}

func (rotatableFile *RotatableFile) Close() error {
	rotatableFile.Mutex.Lock()
	defer rotatableFile.Mutex.Unlock()

	return rotatableFile.file.Close()
}
