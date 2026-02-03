package file

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

func DataFileName(id uint32, width int) string {
	return fmt.Sprintf("%0*d.data", width, id)
}

func HintFileName(id uint32, width int) string {
	return fmt.Sprintf("%0*d.hint", width, id)
}

func IsDataFile(name string) bool {
	return strings.HasSuffix(name, ".data")
}

func IsHintFile(name string) bool {
	return strings.HasSuffix(name, ".hint")
}

func ParseFileID(name string) (uint32, bool) {
	base := filepath.Base(name)
	var trimmed string
	if strings.HasSuffix(base, ".data") {
		trimmed = strings.TrimSuffix(base, ".data")
	} else if strings.HasSuffix(base, ".hint") {
		trimmed = strings.TrimSuffix(base, ".hint")
	} else {
		return 0, false
	}
	id, err := strconv.ParseUint(trimmed, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}
