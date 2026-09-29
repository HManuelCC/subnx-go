package orvanta

import (
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
)

func (c *ClientHardwareResourcesStatistics) GetSystemStats() error {
	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		return err
	}
	if len(cpuPercent) > 0 {
		c.CPUUsage = cpuPercent[0]
	}

	vmStat, err := mem.VirtualMemory()
	if err != nil {
		return err
	}
	c.MemoryUsage = vmStat.UsedPercent

	diskStat, err := disk.Usage("/")
	if err != nil {
		return err
	}
	c.DiskUsage = diskStat.UsedPercent

	ioStart, err := disk.IOCounters()
	if err != nil {
		return err
	}

	time.Sleep(1 * time.Second)

	ioEnd, err := disk.IOCounters()
	if err != nil {
		return err
	}

	for name, start := range ioStart {
		end := ioEnd[name]

		readDelta := end.ReadBytes - start.ReadBytes
		writeDelta := end.WriteBytes - start.WriteBytes

		totalIO := readDelta + writeDelta
		if totalIO > 0 {
			maxThroughput := float64(100 * 1024 * 1024)
			c.DiskBusy = (float64(totalIO) / maxThroughput) * 100
			if c.DiskBusy > 100 {
				c.DiskBusy = 100
			}
		}
		break
	}
	return nil
}
