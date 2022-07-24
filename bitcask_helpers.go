package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func new(directoryPath string, config []Config) (*BitCask, error) {
	var opts Config
	var file *os.File
	var keydir Keydir
	var keydirData []byte
	pendingWrites = make(map[string][]byte)

	if config == nil {
		opts = DefaultConfig
	} else {
		opts = config[0]
	}

	if opts.writePermission {
		file = newFile(directoryPath)
	}
	

	keydirData, err := os.ReadFile(path.Join(directoryPath, hintFileName))
	if err != nil {
		keydir = Keydir{}
	} else {
		keydir = parseKeydirData(string(keydirData))
	}

	bc := &BitCask{
		activeFile: file,
		cursor: 0,
		dirName: directoryPath,
		keydir: keydir,
		config: opts,
	}

	return bc, err
}

func parseKeydirData(keydirData string) Keydir {
	var keydir Keydir = Keydir{}
	var vSz int
	var vPos int
	var t time.Time
	scanner := bufio.NewScanner(strings.NewReader(keydirData))

	for scanner.Scan() {
		line := scanner.Text()
		
		keyAndValue := strings.Split(line, " ")
		key := keyAndValue[0]
		value := keyAndValue[1:]

		vSz, _ = strconv.Atoi(value[1])
		vPos, _ = strconv.Atoi(value[2])
		t, _ = time.Parse(time.RFC3339, value[3])
		
		keydir[key] = Record{
			fileId: value[1],
			valueSize: vSz,
			valuePosition: int64(vPos),
			timeStamp: t,
		}
	}

	return keydir
}

func newFile(directoryPath string) (*os.File){
	var file *os.File
	filename := fmt.Sprintf("%d" + BitCaskFileExtension, time.Now().UnixMilli())

	file, _ = os.OpenFile(path.Join(directoryPath, filename), 
							os.O_CREATE | os.O_WRONLY | os.O_APPEND, 
							UserReadOnly)

	return file
}

func (bc *BitCask) isExist(key []byte) error {
	if _, ok := bc.keydir[string(key)]; !ok {
		return BitCaskError(fmt.Sprintf("%q: %s", string(key), ErrKeyNotExist.Error()))
	}
	return nil
}

func (bc *BitCask) loadToPendingWrites(key, value []byte) error {
	var err error

	if len(pendingWrites) > MaxPendingSize {
		err = bc.Sync()
	}
	pendingWrites[string(key)] = value
	return err
}

func (bc *BitCask) makeItem(key, value []byte, timeStamp time.Time) []byte {
	tStamp := uint32(timeStamp.Unix())
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	item := make([]byte, 16, 16+keySize+valueSize)

	binary.BigEndian.PutUint32(item[4:], tStamp)
	binary.BigEndian.PutUint32(item[8:], keySize)
	binary.BigEndian.PutUint32(item[12:], valueSize)

	item = append(item, key...)
	item = append(item, value...)

	crc := crc32.ChecksumIEEE(item[4:])
	binary.BigEndian.PutUint32(item[:], crc)

	return item
}

func (bc *BitCask) updateKeydirRecord (key, value []byte, fileName string, currentCursorPos int64, tStamp time.Time) {
	bc.keydir[string(key)] = Record {
		fileId: fileName,
		valueSize: len(value),
		valuePosition: int64(currentCursorPos + 16 + int64(len(key))),
		timeStamp: tStamp,
	}
}

func (bc *BitCask) appendItemToFile(item []byte, currentCursorPos *int64, file **os.File) {
	if int64(len(item)) + (*currentCursorPos) > MaxFileSize {
		(*file).Close()

		*file = newFile(bc.dirName)
		*currentCursorPos = 0
	}

	n, _ := (*file).Write(item)
	*currentCursorPos += int64(n)
}

func (bc *BitCask) deleteOldFiles(oldFiles map[string] void) {
	for oldFile := range oldFiles {
		os.Remove(path.Join(bc.dirName, oldFile))
	}
}

