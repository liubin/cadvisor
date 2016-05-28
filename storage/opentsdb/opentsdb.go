// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opentsdb

import (
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"

	opentsdb_config "github.com/bluebreezecf/opentsdb-goclient/config"
	opentsdb "github.com/liubin/opentsdb-goclient/client"
)

func init() {
	storage.RegisterStorageDriver("opentsdb", new)
}

type opentsdbStorage struct {
	client         *opentsdb.Client
	machineName    string
	bufferDuration time.Duration
	lastWrite      time.Time
	points         []*opentsdb.DataPoint
	lock           sync.Mutex
}

// Series names
const (
	// Cumulative CPU usage
	serCpuUsageTotal  string = "cpu.usage.total"
	serCpuUsageSystem string = "cpu.usage.system"
	serCpuUsageUser   string = "cpu.usage.user"
	serCpuUsagePerCpu string = "cpu.usage.per.cpu"
	// Smoothed average of number of runnable threads x 1000.
	serLoadAverage string = "load.average"
	// Memory Usage
	serMemoryUsage string = "memory.usage"
	// Working set size
	serMemoryWorkingSet string = "memory.working.set"
	// Cumulative count of bytes received.
	serRxBytes string = "rx.bytes"
	// Cumulative count of receive errors encountered.
	serRxErrors string = "rx.errors"
	// Cumulative count of bytes transmitted.
	serTxBytes string = "tx.bytes"
	// Cumulative count of transmit errors encountered.
	serTxErrors string = "tx.errors"
	// Filesystem device.
	serFsDevice string = "fs.device"
	// Filesystem limit.
	serFsLimit string = "fs.limit"
	// Filesystem usage.
	serFsUsage string = "fs.usage"
)

func new() (storage.StorageDriver, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return newStorage(
		hostname,
		*storage.ArgDbHost,
		*storage.ArgDbBufferDuration,
	)
}

// Field names
const (
	fieldValue  string = "value"
	fieldType   string = "type"
	fieldDevice string = "device"
)

// Tag names
const (
	tagMachineName   string = "machine"
	tagContainerName string = "container_name"
)

func (self *opentsdbStorage) containerFilesystemStatsToPoints(
	ref info.ContainerReference,
	stats *info.ContainerStats) (points []*opentsdb.DataPoint) {
	if len(stats.Filesystem) == 0 {
		return points
	}
	for _, fsStat := range stats.Filesystem {
		tagsFsUsage := map[string]string{
			fieldDevice: fsStat.Device,
			fieldType:   "usage",
		}

		pointFsUsage := &opentsdb.DataPoint{
			Metric: serFsUsage,
			Tags:   tagsFsUsage,
			Value:  int64(fsStat.Usage),
		}

		tagsFsLimit := map[string]string{
			fieldDevice: fsStat.Device,
			fieldType:   "limit",
		}

		pointFsLimit := &opentsdb.DataPoint{
			Metric: serFsLimit,
			Tags:   tagsFsLimit,
			Value:  int64(fsStat.Limit),
		}

		points = append(points, pointFsUsage, pointFsLimit)
	}

	self.tagPoints(ref, stats, points)

	return points
}

// Set tags and timestamp for all points of the batch.
// Points should inherit the tags that are set for BatchPoints, but that does not seem to work.
func (self *opentsdbStorage) tagPoints(ref info.ContainerReference, stats *info.ContainerStats, points []*opentsdb.DataPoint) {
	// Use container alias if possible
	var containerName string
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	} else {
		containerName = ref.Name
	}

	commonTags := map[string]string{
		tagMachineName:   self.machineName,
		tagContainerName: containerName,
	}
	for i := 0; i < len(points); i++ {
		// merge with existing tags if any
		addTagsToPoint(points[i], commonTags)
		addTagsToPoint(points[i], ref.Labels)
		points[i].Timestamp = stats.Timestamp.Unix()
	}
}

func (self *opentsdbStorage) containerStatsToPoints(
	ref info.ContainerReference,
	stats *info.ContainerStats,
) (points []*opentsdb.DataPoint) {
	// CPU usage: Total usage in nanoseconds
	points = append(points, makePoint(serCpuUsageTotal, stats.Cpu.Usage.Total))

	// CPU usage: Time spend in system space (in nanoseconds)
	points = append(points, makePoint(serCpuUsageSystem, stats.Cpu.Usage.System))

	// CPU usage: Time spent in user space (in nanoseconds)
	points = append(points, makePoint(serCpuUsageUser, stats.Cpu.Usage.User))

	// CPU usage per CPU
	for i := 0; i < len(stats.Cpu.Usage.PerCpu); i++ {
		point := makePoint(serCpuUsagePerCpu, stats.Cpu.Usage.PerCpu[i])
		tags := map[string]string{"instance": fmt.Sprintf("%v", i)}
		addTagsToPoint(point, tags)

		points = append(points, point)
	}

	// Load Average
	points = append(points, makePoint(serLoadAverage, stats.Cpu.LoadAverage))

	// Memory Usage
	points = append(points, makePoint(serMemoryUsage, stats.Memory.Usage))

	// Working Set Size
	points = append(points, makePoint(serMemoryWorkingSet, stats.Memory.WorkingSet))

	// Network Stats
	points = append(points, makePoint(serRxBytes, stats.Network.RxBytes))
	points = append(points, makePoint(serRxErrors, stats.Network.RxErrors))
	points = append(points, makePoint(serTxBytes, stats.Network.TxBytes))
	points = append(points, makePoint(serTxErrors, stats.Network.TxErrors))

	self.tagPoints(ref, stats, points)

	return points
}

func (self *opentsdbStorage) readyToFlush() bool {
	return time.Since(self.lastWrite) >= self.bufferDuration
}

func (self *opentsdbStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	if stats == nil {
		return nil
	}
	var pointsToFlush []*opentsdb.DataPoint
	func() {
		// AddStats will be invoked simultaneously from multiple threads and only one of them will perform a write.
		self.lock.Lock()
		defer self.lock.Unlock()

		self.points = append(self.points, self.containerStatsToPoints(ref, stats)...)
		self.points = append(self.points, self.containerFilesystemStatsToPoints(ref, stats)...)
		if self.readyToFlush() {
			pointsToFlush = self.points
			self.points = make([]*opentsdb.DataPoint, 0)
			self.lastWrite = time.Now()
		}
	}()
	if len(pointsToFlush) > 0 {
		points := make([]opentsdb.DataPoint, len(pointsToFlush))
		for i, p := range pointsToFlush {
			points[i] = *p
		}

		// batchTags := map[string]string{tagMachineName: self.machineName}
		// bp := influxdb.BatchPoints{
		// 	Points:   points,
		// 	Database: self.database,
		// 	Tags:     batchTags,
		// 	Time:     stats.Timestamp,
		// }
		// fmt.Printf("datapoint is %v\n", points)

		if resp, err := (*self.client).Put(points, "details"); err != nil {
			fmt.Printf("Error occurs when putting datapoints: %v", err)
		} else {
			fmt.Printf("%s", resp.String())
		}

	}
	return nil
}

func (self *opentsdbStorage) Close() error {
	self.client = nil
	return nil
}

// machineName: A unique identifier to identify the host that current cAdvisor
// instance is running on.
// opentsdbHost: The host which runs OpenTSDB (host:port)
func newStorage(
	machineName,
	opentsdbHost string,
	bufferDuration time.Duration,
) (*opentsdbStorage, error) {

	fmt.Printf("OpenTSDB host %v\n", opentsdbHost)

	opentsdbCfg := opentsdb_config.OpenTSDBConfig{
		OpentsdbHost: opentsdbHost,
	}

	tsdbClient, err := opentsdb.NewClient(opentsdbCfg)
	if err != nil {
		fmt.Printf("%v\n", err)
		return nil, err
	}

	//0. Ping
	if err = tsdbClient.Ping(); err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	storage := &opentsdbStorage{
		client:         &tsdbClient,
		machineName:    machineName,
		bufferDuration: bufferDuration,
		lastWrite:      time.Now(),
		points:         make([]*opentsdb.DataPoint, 0),
	}

	return storage, nil
}

// Creates a measurement point with a single value field
func makePoint(name string, value interface{}) *opentsdb.DataPoint {
	return &opentsdb.DataPoint{
		Metric: name,
		Value:  value,
	}
}

// Adds additional tags to the existing tags of a point
func addTagsToPoint(point *opentsdb.DataPoint, tags map[string]string) {
	if point.Tags == nil {
		point.Tags = make(map[string]string, 0)
	}

	// TODO move somewhere else.
	rep := regexp.MustCompile(`[^0-9a-zA-Z-_/\.]`)

	// Characters are allowed:
	// a to z, A to Z, 0 to 9, -, _, ., /
	// or Unicode letters (as per the specification)
	for k, v := range tags {
		point.Tags[k] = rep.ReplaceAllString(v, "-")
	}
}
