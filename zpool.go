package zfs

import (
	"strings"
)

// ZFS zpool states, which can indicate if a pool is online, offline, degraded, etc.
//
// More information regarding zpool states can be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zpoolconcepts.7.html#Device_Failure_and_Recovery
const (
	ZpoolOnline    = "ONLINE"
	ZpoolDegraded  = "DEGRADED"
	ZpoolFaulted   = "FAULTED"
	ZpoolOffline   = "OFFLINE"
	ZpoolUnavail   = "UNAVAIL"
	ZpoolRemoved   = "REMOVED"
	ZpoolDestroyed = "ONLINE (DESTROYED)"
)

// Zpool is a ZFS zpool.
// A pool is a top-level structure in ZFS, and can contain many descendent datasets.
type Zpool struct {
	Name          string
	Health        string
	Allocated     uint64
	Size          uint64
	Free          uint64
	Fragmentation uint64
	ReadOnly      bool
	Freeing       uint64
	Leaked        uint64
	DedupRatio    float64
	Vdevs         []VdevGroup
}

// zpool is a helper function to wrap typical calls to zpool and ignores stdout.
func zpool(arg ...string) error {
	_, err := zpoolOutput(arg...)
	return err
}

// zpool is a helper function to wrap typical calls to zpool.
func zpoolOutput(arg ...string) ([][]string, error) {
	c := command{Command: "zpool"}
	return c.Run(arg...)
}

// GetZpool retrieves a single ZFS zpool by name.
func GetZpool(name string) (*Zpool, error) {
	args := zpoolArgs
	args = append(args, name)
	out, err := zpoolOutput(args...)
	if err != nil {
		return nil, err
	}

	z := &Zpool{Name: name}
	for _, line := range out {
		if err := z.parseLine(line); err != nil {
			return nil, err
		}
	}

	// Retrieve details of associated vdevs
	args = zpoolVdevArgs
	args = append(args, name)
	out, err = zpoolOutput(args...)
	if err != nil {
		return nil, err
	}
	if err := z.parseVdevs(out); err != nil {
		return nil, err
	}

	return z, nil
}

// Datasets returns a slice of all ZFS datasets in a zpool.
func (z *Zpool) Datasets() ([]*Dataset, error) {
	return Datasets(z.Name)
}

// Snapshots returns a slice of all ZFS snapshots in a zpool.
func (z *Zpool) Snapshots() ([]*Dataset, error) {
	return Snapshots(z.Name)
}

// CreateZpool creates a new ZFS zpool with the specified name, properties, and optional arguments.
//
// A full list of available ZFS properties and command-line arguments may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
// https://openzfs.github.io/openzfs-docs/man/8/zpool-create.8.html
func CreateZpool(name string, properties map[string]string, args ...string) (*Zpool, error) {
	cli := make([]string, 1, 4)
	cli[0] = "create"
	if properties != nil {
		cli = append(cli, propsSlice(properties)...)
	}
	cli = append(cli, name)
	cli = append(cli, args...)
	if err := zpool(cli...); err != nil {
		return nil, err
	}

	return &Zpool{Name: name}, nil
}

// Destroy destroys a ZFS zpool by name.
func (z *Zpool) Destroy() error {
	err := zpool("destroy", z.Name)
	return err
}

// ListZpools list all ZFS zpools accessible on the current system.
func ListZpools() ([]*Zpool, error) {
	args := []string{"list", "-Ho", "name"}
	out, err := zpoolOutput(args...)
	if err != nil {
		return nil, err
	}

	var pools []*Zpool

	for _, line := range out {
		z, err := GetZpool(line[0])
		if err != nil {
			return nil, err
		}
		pools = append(pools, z)
	}
	return pools, nil
}

// ExportedZpool is a ZFS zpool that can be imported.
// A pool is a top-level structure in ZFS, and can contain many descendent datasets.
type ExportedZpool struct {
	Name   string
	Id     string
	State  string
	Status string
	Action string
	Vdevs  []VdevGroup
}

func (ez *ExportedZpool) Import(tryForce bool) error {
	importFlags := "-N"
	if tryForce {
		importFlags = importFlags + "f"
	}
	if strings.Contains(ez.State, "DESTROYED") {
		importFlags = importFlags + "D"
	}
	args := []string{"import", importFlags, ez.Id}
	_, err := zpoolOutput(args...)
	return err
}

func (ez *ExportedZpool) parseLines(lines [][]string) int {
	var curVdevGroup *VdevGroup

	actionFound := false
	configFound := false
	loc := 0
	curVdevGroup = nil
	for _, line := range lines {
		loc = loc + 1

		if len(line) == 0 {
			continue
		}
		switch line[0] {
		case "pool:":
			return loc - 1
		case "id:":
			setString(&ez.Id, line[1])
		case "state:":
			setString(&ez.State, strings.Join(line[1:], " "))
		case "status:":
			setString(&ez.Status, strings.Join(line[1:], " "))
		case "action:":
			setString(&ez.Action, strings.Join(line[1:], " "))
			actionFound = true
		case ez.Name:
			continue
		case "config:":
			configFound = true
			continue
		default:
			if actionFound && !configFound {
				ez.Action = ez.Action + " " + strings.Join(line, " ")
				actionFound = false
				continue
			}

			// example: raidz1-0
			if IsVdevGroup(strings.Split(line[0], "-")[0]) {
				curVdevGroup = &VdevGroup{
					Group: Vdev{
						Name:   ResolveDeviceName(line[0]), // TODO: Use stat here.
						Health: line[1],
					},
\				}
				ez.Vdevs = append(ez.Vdevs, curVdevGroup)
			} else {
				if curVdevGroup == nil {
					curVdevGroup = &VdevGroup{
						Group: Vdev{
							Name: "fileordisk",
						},
					}
					ez.Vdevs = append(ez.Vdevs, curVdevGroup)
				}
				device := Vdev{
					Name:   ResolveDeviceName(line[0]),
					Health: line[1],
				}
				curVdevGroup.Devices = append(curVdevGroup.Devices, device)
			}
		}
	}
	return loc
}

// ListExportedZpools list all ZFS zpools that can be imported on the current system.
func ListExportedZpools() ([]*ExportedZpool, error) {
	args := []string{"import"}
	out, err := zpoolOutput(args...)
	if err != nil {
		return nil, err
	}
	var pools []*ExportedZpool
	for i := 0; i < len(out); i++ {
		ez := &ExportedZpool{}
		if out[i][0] == "pool:" {
			ez.Name = out[i][1]
			linesParsed := ez.parseLines(out[i+1:])
			i = i + linesParsed
			pools = append(pools, ez)
		}
	}

	args = []string{"import", "-D"}
	out, err = zpoolOutput(args...)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(out); i++ {
		ez := &ExportedZpool{}
		if out[i][0] == "pool:" {
			ez.Name = out[i][1]
			linesParsed := ez.parseLines(out[i+1:])
			i = i + linesParsed
			pools = append(pools, ez)
		}
	}
	return pools, nil
}
