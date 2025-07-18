package sibylla

import (
	"log"
	"sort"
)

type accessorBuffer struct {
	accessor featureAccessor
	buffer []featureIndex
}

type featureIndex struct {
	value float64
	index int
}

func quantileTransform(bufferSize, stride int, input []FeatureRecord) []FeatureRecord {
	bufferSize = min(bufferSize, len(input))
	if stride >= bufferSize {
		log.Fatalf("Invalid stride for quantile transform (stride = %d, bufferSize = %d)", stride, bufferSize)
	}
	accessors := getFeatureAccessors()
	accessorBuffers := []accessorBuffer{}
	for _, accessor := range accessors {
		accBuffer := accessorBuffer{
			accessor: accessor,
			buffer: make([]featureIndex, 0, bufferSize),
		}
		accessorBuffers = append(accessorBuffers, accBuffer)
	}
	output := make([]FeatureRecord, len(input))
	copy(output, input)
	writeQuantileRecords(0, bufferSize, bufferSize, accessorBuffers, input, output)
	for offset := stride; offset + bufferSize < len(input); offset += stride {
		writeQuantileRecords(offset, bufferSize, stride, accessorBuffers, input, output)
	}
	writeQuantileRecords(len(input) - bufferSize, bufferSize, stride, accessorBuffers, input, output)
	return output
}

func writeQuantileRecords(
	offset int,
	bufferSize int,
	updateRange int,
	accessorBuffers []accessorBuffer,
	input []FeatureRecord,
	output []FeatureRecord,
) {
	for i := range accessorBuffers {
		accBuffer := &accessorBuffers[i]
		accBuffer.buffer = accBuffer.buffer[:0]
	}
	for i, record := range input[offset:offset + bufferSize] {
		for j := range accessorBuffers {
			accBuffer := &accessorBuffers[j]
			value := accBuffer.accessor.get(&record)
			if value != nil {
				featIndex := featureIndex{
					value: *value,
					index: offset + i,
				}
				accBuffer.buffer = append(accBuffer.buffer, featIndex)
			}
		}
	}
	for i := range accessorBuffers {
		accBuffer := &accessorBuffers[i]
		sort.Slice(accBuffer.buffer, func (i, j int) bool {
			return accBuffer.buffer[i].value < accBuffer.buffer[j].value
		})
		for j, featIndex := range accBuffer.buffer {
			if featIndex.index >= offset + bufferSize - updateRange {
				destination := &output[featIndex.index]
				quantile := float64(j) / float64(len(accBuffer.buffer) - 1)
				accBuffer.accessor.set(destination, quantile)
			}
		}
	}
}
