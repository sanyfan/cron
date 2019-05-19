// Package cron implements a cron spec parser and runner.
package cron // import "gopkg.in/robfig/cron.v2"

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bsm/redis-lock"
	"github.com/spf13/viper"
	"reflect"
	"sort"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries    []*Entry
	stop       chan struct{}
	add        chan *Entry
	remove     chan EntryID
	removeName chan string
	snapshot   chan []Entry
	running    bool
	nextID     EntryID
	mode       string
	ns         string
	funcList   map[string]interface{}
}

type FuncInfo struct {
	name   string
	params []interface{}
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

	// The Job's func name
	FuncName string

	// The Job's func param
	params []interface{}
}

const (
	ModeNormal = "normal"
	ModeRedis  = "redis"
)

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
		entries:    nil,
		add:        make(chan *Entry),
		stop:       make(chan struct{}),
		snapshot:   make(chan []Entry),
		remove:     make(chan EntryID),
		removeName: make(chan string),
		running:    false,
		mode:       ModeNormal,
	}
}

func NewPoolWithRedis(v *viper.Viper, ns string, funcList map[string]interface{}) *Cron {
	err := InitRedisClient(v, ns)
	if err != nil {
		fmt.Println("Init Redis Err")
		return New()
	}
	cron := &Cron{
		entries:    nil,
		add:        make(chan *Entry),
		stop:       make(chan struct{}),
		snapshot:   make(chan []Entry),
		remove:     make(chan EntryID),
		removeName: make(chan string),
		running:    false,
		mode:       ModeRedis,
		ns:         ns,
		funcList:   funcList,
	}
	RedisCli.Set(buildCronKey(ns), "", 0)
	cron.AddFunc("5 * * * * *", time.Now().Format("2006-01-02"), cron.syncCrons, true)
	return cron
}

func buildCronKey(ns string) string {
	return fmt.Sprintf("%s:%s", "cronEntries", ns)
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFuncInRedis adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFuncInRedis(spec, start, funcName string, cmd func(), unique bool, params []interface{}, names ...string) (EntryID, error) {
	var name string
	if len(names) <= 0 {
		name = fmt.Sprintf("%d", time.Now().Unix())
	} else {
		name = names[0]
	}
	return c.AddJob(spec, start, funcName, FuncJob(cmd), unique, params, name)
}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec, start string, cmd func(), unique bool, names ...string) (EntryID, error) {
	var name string
	if len(names) <= 0 {
		name = fmt.Sprintf("%d", time.Now().Unix())
	} else {
		name = names[0]
	}
	return c.AddJob(spec, start, "", FuncJob(cmd), unique, nil, name)

}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddOnceFunc(spec, start string, cmd func(), names ...string) (EntryID, error) {
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

	return c.AddJob(spec, start, "", FuncJob(onceCmd), true, nil, name)
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec, start, funcName string, cmd Job, unique bool, params []interface{}, names ...string) (EntryID, error) {
	schedule, err := Parse(spec, start)
	if err != nil {
		return 0, err
	}
	var name string
	if len(names) <= 0 {
		name = fmt.Sprintf("%d", time.Now().Unix())
	} else {
		name = names[0]
	}
	if unique && len(names) > 0 {
		c.RemoveByName(name)
	}
	return c.Schedule(schedule, cmd, spec, funcName, params, name), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job, spec, funcName string, params []interface{}, names ...string) EntryID {
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
		FuncName: funcName,
		params:   params,
	}
	if !c.running {
		c.addEntry(entry)
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

func (c *Cron) syncCrons() {
	set := c.getNameSet()
	entries := c.getEntries()
	for i, v := range set {
		if entryExists(entries, i) {
			continue
		}
		if v.name == "" {
			return
		}
		f, ok := c.funcList[v.name]
		if ok {
			exec(f, v.params)
		}
	}
}

func exec(function interface{}, params []interface{}) {
	f := reflect.ValueOf(function)
	if len(params) != f.Type().NumIn() {
		fmt.Println("The number of params is not adapted.")
		return
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	f.Call(in)
}

func entryExists(entries []*Entry, name string) bool {
	for _, v := range entries {
		if v.Name == name {
			return true
		}
	}
	return false
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

	for {
		entries := c.getEntries()
		for _, entry := range entries {
			entry.Next = entry.Schedule.Next(now)
		}
		// Determine the next entry to run.
		sort.Sort(byTime(entries))

		var effective time.Time
		if len(entries) == 0 || entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = entries[0].Next
		}

		select {
		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			for _, e := range entries {
				if e.Next != effective {
					break
				}
				go e.Job.Run()
				e.Prev = e.Next
				e.Next = e.Schedule.Next(effective)
			}
			continue

		case newEntry := <-c.add:
			now = time.Now().Local()
			c.addEntry(newEntry)
			// newEntry.Next = newEntry.Schedule.Next(now)

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case id := <-c.remove:
			c.removeEntry(id)

		case name := <-c.removeName:
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
	entries := c.getEntries()
	var re = make([]Entry, len(entries))
	for i, e := range entries {
		re[i] = *e
	}
	return re
}

func (c *Cron) addEntry(e *Entry) {
	if c.mode == ModeNormal {
		c.entries = append(c.entries, e)
		return
	}
	lock, err := c.newLock()
	if err != nil {
		return
	}
	defer lock.Unlock()
	entries := c.getEntries()
	entries = append(entries, e)
	c.setEntries(entries)
	if c.nameExists(e.Name) {
		fmt.Println("Entry Name already exists")
		return
	}
	c.addNameSet(e.Name, e.FuncName, e.params)
	return
}

func (c *Cron) removeEntry(id EntryID) {
	entries := c.getEntries()
	var re []*Entry
	if c.mode == ModeNormal {
		for _, e := range entries {
			if e.ID != id {
				re = append(re, e)
			}
		}
		c.setEntries(re)
		return
	}

	lock, err := c.newLock()
	if err != nil {
		return
	}
	defer lock.Unlock()
	for _, e := range entries {
		if e.ID != id {
			re = append(re, e)
		} else {
			c.removeNameSet(e.Name)
		}
	}
	c.setEntries(re)
}

func (c *Cron) removeEntryByName(name string) {
	var re []*Entry
	entries := c.getEntries()
	if c.mode == ModeNormal {
		for _, e := range entries {
			if e.Name != name {
				re = append(re, e)
			}
		}
		c.setEntries(re)
		return
	}
	lock, err := c.newLock()
	if err != nil {
		return
	}
	defer lock.Unlock()
	for _, e := range entries {
		if e.Name != name {
			re = append(re, e)
		}
	}
	c.setEntries(re)
	c.removeNameSet(name)
}

func (c *Cron) getEntries() []*Entry {
	// if c.mode == "normal" {
	return c.entries
	// }
	// entriesStr, err := RedisCli.GetString(buildCronKey(c.ns))
	// if err != nil {
	// 	fmt.Println("Get Entries from Redis Err: ", err)
	// 	return nil
	// }
	// var re []*Entry
	// err = json.Unmarshal([]byte(entriesStr), &re)
	// if err != nil {
	// 	fmt.Println("Unmarshal Entries from Redis Err: ", err)
	// 	return nil
	// }
	// return re
}

func (c *Cron) setEntries(e []*Entry) error {
	// if c.mode == "normal" {
	c.entries = e
	return nil
	// }
	// entriesStr, _ := json.Marshal(e)
	// _, err := RedisCli.Set(buildCronKey(c.ns), string(entriesStr), 0)
	// return err
}

func (c *Cron) getNameSet() map[string]FuncInfo {
	if c.mode == ModeNormal {
		return nil
	}
	entriesStr, err := RedisCli.Get(buildCronKey(c.ns)).Result()
	if err != nil {
		fmt.Println("Get Entries from Redis Err: ", err)
		return nil
	}
	fmt.Println("entryStr: ", entriesStr)
	if entriesStr == "" {
		return make(map[string]FuncInfo, 0)
	}
	var re map[string]FuncInfo
	err = json.Unmarshal([]byte(entriesStr), &re)
	if err != nil {
		fmt.Println("Unmarshal Entries from Redis Err: ", err)
		return nil
	}
	return re
}

func (c *Cron) setNameSet(s map[string]FuncInfo) error {
	setStr, _ := json.Marshal(s)
	_, err := RedisCli.Set(buildCronKey(c.ns), string(setStr), 0)
	return err
}

func (c *Cron) removeNameSet(name string) error {
	nameSet := c.getNameSet()
	delete(nameSet, name)
	return c.setNameSet(nameSet)
}

func (c *Cron) addNameSet(name, funcName string, params []interface{}) error {
	nameSet := c.getNameSet()
	if !c.nameExists(name) {
		nameSet[name] = FuncInfo{
			name:   funcName,
			params: params,
		}
		return c.setNameSet(nameSet)
	}
	return errors.New("name already exists")
}

func (c *Cron) nameExists(name string) bool {
	nameSet := c.getNameSet()
	_, ok := nameSet[name]
	return ok
}

func (c *Cron) newLock() (*lock.Locker, error) {
	lock, err := lock.Obtain(RedisCli.Client, buildCronKey(c.ns), nil)
	if err != nil {
		fmt.Println("Obtain Redis Lock Err: ", err)
		return nil, err
	}
	return lock, nil
}
