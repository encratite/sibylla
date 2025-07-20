package sibylla

import (
	"fmt"
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
	output := make([]FeatureRecord, len(input))
	copy(output, input)
	// outputTest(output)
	anchoredQuantileTransform(bufferSize, input, output)
	// outputTest(output)
	rollingQuantileTransform(bufferSize, stride, input, output)
	return output
}

func outputTest(output []FeatureRecord) {
	for i, record := range output[:100] {
		if record.Momentum1DLag != nil {
			fmt.Printf("output[%d] = %.4f\n", i, *record.Momentum1DLag)
		} else {
			fmt.Printf("output[%d] = nil\n", i)
		}
	}
}

func anchoredQuantileTransform(
	bufferSize int,
	input []FeatureRecord,
	output []FeatureRecord,
) {
	accessors := getFeatureAccessors()
	accessorBuffers := []accessorBuffer{}
	for _, accessor := range accessors {
		if accessor.anchored {
			accBuffer := newAccessorBuffer(len(input), accessor)
			accessorBuffers = append(accessorBuffers, accBuffer)
		}
	}
	filAccessorBuffers(0, bufferSize, input, accessorBuffers)
	for i := range accessorBuffers {
		accBuffer := &accessorBuffers[i]
		accBuffer.sort()
		for i, featIndex := range accBuffer.buffer {
			accBuffer.apply(i, featIndex, output)
		}
	}
	for i, record := range input[bufferSize:] {
		for j := range accessorBuffers {
			accBuffer := &accessorBuffers[j]
			value := accBuffer.accessor.get(&record)
			if value != nil {
				featIndex := featureIndex{
					value: *value,
					index: bufferSize + i,
				}
				insertIndex := accBuffer.insert(featIndex)
				accBuffer.apply(insertIndex, featIndex, output)
			}
		}
	}
	for i := range output[:bufferSize] {
		output[i].Momentum1DLag = nil
	}
}

func rollingQuantileTransform(
	bufferSize int,
	stride int,
	input []FeatureRecord,
	output []FeatureRecord,
) {
	accessors := getFeatureAccessors()
	accessorBuffers := []accessorBuffer{}
	for _, accessor := range accessors {
		if !accessor.anchored {
			accBuffer := newAccessorBuffer(bufferSize, accessor)
			accessorBuffers = append(accessorBuffers, accBuffer)
		}
	}
	writeQuantileRecords(0, bufferSize, bufferSize, accessorBuffers, input, output)
	for offset := stride; offset + bufferSize < len(input); offset += stride {
		writeQuantileRecords(offset, bufferSize, stride, accessorBuffers, input, output)
	}
	writeQuantileRecords(len(input) - bufferSize, bufferSize, stride, accessorBuffers, input, output)
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
	filAccessorBuffers(offset, bufferSize, input, accessorBuffers)
	for i := range accessorBuffers {
		accBuffer := &accessorBuffers[i]
		accBuffer.sort()
		for j, featIndex := range accBuffer.buffer {
			if featIndex.index >= offset + bufferSize - updateRange {
				accBuffer.apply(j, featIndex, output)
			}
		}
	}
}

func newAccessorBuffer(bufferSize int, accessor featureAccessor) accessorBuffer {
	return accessorBuffer{
		accessor: accessor,
		buffer: make([]featureIndex, 0, bufferSize),
	}
}

func (accBuffer *accessorBuffer) sort() {
	sort.Slice(accBuffer.buffer, func (i, j int) bool {
		return accBuffer.buffer[i].value < accBuffer.buffer[j].value
	})
}

func (accBuffer *accessorBuffer) apply(i int, featIndex featureIndex, output []FeatureRecord) {
	destination := &output[featIndex.index]
	quantile := float64(i) / float64(len(accBuffer.buffer) - 1)
	// fmt.Printf("output[%d] = %d / %d = %.2f\n", featIndex.index, i, len(accBuffer.buffer) - 1, quantile)
	accBuffer.accessor.set(destination, quantile)
}

func (accBuffer *accessorBuffer) insert(featIndex featureIndex) int {
	insertIndex := sort.Search(len(accBuffer.buffer), func (i int) bool {
		return accBuffer.buffer[i].value >= featIndex.value
	})
	fmt.Printf("Pre: %s | %s\n", accBuffer.buffer[insertIndex:insertIndex + 10], accBuffer.buffer[len(accBuffer.buffer) - 1])
	accBuffer.buffer = append(accBuffer.buffer, featureIndex{})
	copy(accBuffer.buffer[insertIndex + 1:], accBuffer.buffer[insertIndex:])
	accBuffer.buffer[insertIndex] = featIndex
	fmt.Printf("Post: %s | %s\n", accBuffer.buffer[insertIndex:insertIndex + 10], accBuffer.buffer[len(accBuffer.buffer) - 1])
	return insertIndex
}

func filAccessorBuffers(
	offset int,
	bufferSize int,
	input []FeatureRecord,
	accessorBuffers []accessorBuffer,
) {
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
}

func (f featureIndex) String() string {
	return fmt.Sprintf("(%.4f, %d)", f.value, f.index)
}