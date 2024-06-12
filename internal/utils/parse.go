package utils

import (
	"fmt"
	"strconv"
)

func ParseSize(size string) (int, error) {
	unitMultipliers := map[string]int{
		"B":  1,
		"KB": 1024,
		"MB": 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"TB": 1024 * 1024 * 1024 * 1024,
	}

	var letterIdx int
	for i := 0; i < len(size); i++ {
		if size[i] < '0' || size[i] > '9' {
			letterIdx = i
			break
		}
	}

	sizeNum, err := strconv.Atoi(size[:letterIdx])
	if err != nil {
		return 0, fmt.Errorf("config. invalid size: %w", err)
	}
	unit := size[letterIdx:]
	multiplier, _ := unitMultipliers[unit]

	return sizeNum * multiplier, nil
}
