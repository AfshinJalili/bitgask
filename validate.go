package bitgask

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/AfshinJalili/bitgask/internal/file"
	"github.com/AfshinJalili/bitgask/internal/index"
	"github.com/AfshinJalili/bitgask/internal/record"
)

type Report struct {
	DataFiles      int
	HintFiles      int
	Records        int
	CorruptRecords int
	Errors         []error
}

func (r Report) HasErrors() bool {
	return r.CorruptRecords > 0 || len(r.Errors) > 0
}

func Validate(path string, opts Options) (Report, error) {
	opts = opts.withDefaults()
	if path == "" {
		return Report{}, fmt.Errorf("bitgask: path required")
	}
	dataDir := filepath.Join(path, "data")
	lock, err := file.AcquireLock(filepath.Join(dataDir, lockFileName), opts.LockTimeout)
	if err != nil {
		return Report{}, ErrLocked
	}
	defer func() { _ = lock.Release() }()

	files, err := listDataFiles(dataDir)
	if err != nil {
		return Report{}, err
	}
	var report Report
	report.DataFiles = len(files)
	for _, id := range files {
		path := filepath.Join(dataDir, file.DataFileName(id, opts.FileIDWidth))
		f, err := os.Open(path)
		if err != nil {
			report.Errors = append(report.Errors, err)
			continue
		}
		reader := bufio.NewReader(f)
		for {
			rec, _, err := record.DecodeFrom(reader)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				if errors.Is(err, record.ErrCorrupt) {
					report.CorruptRecords++
					break
				}
				report.Errors = append(report.Errors, err)
				break
			}
			report.Records++
			_ = rec
		}
		_ = f.Close()
	}

	if opts.UseHintFiles {
		for _, id := range files {
			path := filepath.Join(dataDir, file.HintFileName(id, opts.FileIDWidth))
			hintFile, err := os.Open(path)
			if err != nil {
				report.Errors = append(report.Errors, err)
				continue
			}
			report.HintFiles++
			reader := bufio.NewReader(hintFile)
			for {
				entry, _, err := index.ReadHint(reader)
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						break
					}
					report.Errors = append(report.Errors, err)
					break
				}
				// Spot check that hint entry points to matching key
				dataPath := filepath.Join(dataDir, file.DataFileName(entry.FileID, opts.FileIDWidth))
				dataFile, err := os.Open(dataPath)
				if err != nil {
					report.Errors = append(report.Errors, err)
					continue
				}
				rec, _, err := record.ReadAt(dataFile, entry.Offset)
				_ = dataFile.Close()
				if err != nil {
					report.Errors = append(report.Errors, err)
					continue
				}
				if string(rec.Key) != string(entry.Key) {
					report.Errors = append(report.Errors, fmt.Errorf("hint key mismatch at file %d offset %d", entry.FileID, entry.Offset))
				}
			}
			_ = hintFile.Close()
		}
	}

	return report, nil
}
