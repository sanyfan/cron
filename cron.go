// Package cron implements a cron spec parser and runner.
package cron // import "gopkg.in/robfig/cron.v2"

import (
	"sort"
	"time"
	"fmt"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries  []*Entry
	stop     chan struct{}
	add      chan *Entry
	remove   chan EntryID
	removeName chan string
	snapshot chan []Entry
	running  bool
	nextID   EntryID
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// Spec is the spec setting
	Spec string

	// Job is the thing to run when the Schedule is activated.
	Job Job

	// The Job's name
	Name string
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner.
func New() *Cron {
	return &Cron{
		entries:  nil,
		add:      make(chan *Entry),
		stop:     make(chan struct{}),
		snapshot: make(chan []Entry),
		remove:   make(chan EntryID),
		removeName:make(chan string),
		running:  false,
	}
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func(),names ...string) (EntryID, error) {
	var name string
	if len(names) <= 0 {
		name = fmt.Sprintf("%d", time.Now().Unix())
	} else {
		name = names[0]
	}
	return c.AddJob(spec, FuncJob(cmd),name)
}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddOnceFunc(spec string, cmd func(), names ...string) (EntryID, error) {
	var name string
	if len(names) <= 0 {
		name = fmt.Sprintf("%d", time.Now().Unix())
	} else {
		name = names[0]
	}

	// Support Once run function.
	onceCmd := func() {
		cmd()
		c.RemoveByName(name)
	}

	return c.AddJob(spec, FuncJob(onceCmd), name)
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job,names ...string) (EntryID, error) {
	schedule, err := Parse(spec)
	if err != nil {
		return 0, err
	}
	var name string
	if len(names) <= 0 {
		name = fmt.Sprintf("%d", time.Now().Unix())
	} else {
		name = names[0]
	}
	return c.Schedule(schedule, cmd,spec,name), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job,spec string,names ...string) EntryID {
	var name string
	if len(names) <= 0 {
		name = fmt.Sprintf("%d", time.Now().Unix())
	} else {
		name = names[0]
	}
	c.nextID++
	entry := &Entry{
		ID:       c.nextID,
		Schedule: schedule,
		Job:      cmd,
		Name:     name,
		Spec:     spec,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	if c.running {
		c.snapshot <- nil
		return <-c.snapshot
	}
	return c.entrySnapshot()
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// EntryByName returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) EntryByName(name string) Entry {
	for _, entry := range c.Entries() {
		if name == entry.Name {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Remove an entry from being run in the future.
func (c *Cron) RemoveByName(name string) {
	if c.running {
		c.removeName <- name
	} else {
		c.removeEntryByName(name)
	}
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
	c.running = true
	go c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := time.Now().Local()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var effective time.Time
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = c.entries[0].Next
		}

		select {
		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			for _, e := range c.entries {
				if e.Next != effective {
					break
				}
				go e.Job.Run()
				e.Prev = e.Next
				e.Next = e.Schedule.Next(effective)
			}
			continue

		case newEntry := <-c.add:
			c.entries = append(c.entries, newEntry)
			newEntry.Next = newEntry.Schedule.Next(now)

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case id := <-c.remove:
			c.removeEntry(id)

		case name:= <- c.removeName:
			c.removeEntryByName(name)

		case <-c.stop:
			return
		}

		now = time.Now().Local()
	}
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}

func (c *Cron) removeEntryByName(name string) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.Name != name {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}
