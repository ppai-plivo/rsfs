package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	redis "github.com/go-redis/redis/v7"
)

func newRedisClient(endpoints []string) (redis.UniversalClient, error) {

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: endpoints,
	})

	if _, err := client.Ping().Result(); err != nil {
		return nil, err
	}

	return client, nil
}

type redisFS struct {
	client       redis.UniversalClient
	attrValidity time.Duration
}

func (rfs *redisFS) Root() (fs.Node, error) {
	return &redisDir{
		root:    true,
		redisFS: rfs,
	}, nil
}

func (rfs *redisFS) GenerateInode(parentInode uint64, name string) uint64 {
	h := fnv.New64a()
	b := make([]byte, binary.MaxVarintLen64)
	binary.LittleEndian.PutUint64(b, parentInode)
	h.Write([]byte(name))
	return h.Sum64()
}

type redisDir struct {
	root    bool
	name    string
	t       string
	entries []fuse.Dirent
	names   map[string]struct{}
	*redisFS
}

func (d *redisDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Valid = d.attrValidity
	a.Mode = os.ModeDir | 0555
	if d.root == true {
		a.Inode = 1
	}
	return nil
}

func (d *redisDir) Lookup(ctx context.Context, name string) (fs.Node, error) {

	ok, err := d.client.Exists(name).Result()
	if err == redis.Nil || ok != 1 {
		return nil, syscall.ENOENT
	}
	if err != nil {
		return nil, syscall.EIO
	}

	t, err := d.client.Type(name).Result()
	if err == redis.Nil || ok != 1 {
		return nil, syscall.ENOENT
	}
	if err != nil {
		return nil, syscall.EIO
	}

	if t == "stream" {
		return &redisDir{
			name:    name,
			redisFS: d.redisFS,
			t:       "stream",
		}, nil
	}

	return &redisFile{
		name:    name,
		redisFS: d.redisFS,
	}, nil
}

func (d *redisDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	if d.root {
		keys, err := d.client.Keys("*").Result()
		if err != nil {
			return nil, syscall.EIO
		}

		entries := make([]fuse.Dirent, len(keys))
		for i := 0; i < len(keys); i++ {
			entries[i].Name = keys[i]
			t, err := d.client.Type(keys[i]).Result()
			if err != nil {
				return nil, syscall.EIO
			}
			if t == "stream" {
				entries[i].Type = fuse.DT_Dir
			} else if t == "string" {
				entries[i].Type = fuse.DT_File
			}
		}

		return entries, nil
	}

	return nil, nil
}

func (d *redisDir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	resp.Flags |= fuse.OpenDirectIO

	f := &redisFile{
		parent:  d.name,
		name:    req.Name,
		redisFS: d.redisFS,
	}

	return f, f, nil
}

func (d *redisDir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	xAddArgs := &redis.XAddArgs{
		Stream: req.Name,
		Values: map[string]interface{}{
			"blob": "dummy",
		},
		ID: "0-1",
	}

	_, err := d.client.XAdd(xAddArgs).Result()
	if err != nil {
		fmt.Println("Mkdir:XAdd", err, xAddArgs.Stream, xAddArgs.ID)
		return nil, syscall.EIO
	}

	_, err = d.client.XDel(xAddArgs.Stream, xAddArgs.ID).Result()
	if err != nil {
		fmt.Println("Mkdir:XDel", err, xAddArgs.Stream, xAddArgs.ID)
		return nil, syscall.EIO
	}

	return &redisDir{
		name:    req.Name,
		redisFS: d.redisFS,
		t:       "stream",
	}, nil
}

type redisFile struct {
	name   string
	parent string
	size   uint64
	rb     []byte
	wb     []byte
	ro     bool
	mu     sync.RWMutex
	*redisFS
}

func (f *redisFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if req.Flags.IsReadOnly() && !req.Dir {
		f.ro = false
	}
	resp.Flags |= fuse.OpenDirectIO
	return f, nil
}

func (f *redisFile) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wb = append(f.wb, req.Data...)
	resp.Size = len(req.Data)
	return nil
}

func (f *redisFile) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ro {
		return nil
	}

	if f.parent != "" {
		// stream
		xAddArgs := &redis.XAddArgs{
			Stream: f.parent,
			Values: map[string]interface{}{
				"blob": f.wb,
			},
			ID: f.name + "-0",
		}

		_, err := f.client.XAdd(xAddArgs).Result()
		if err != nil {
			fmt.Println("Flush:XAdd", err, xAddArgs.Stream, xAddArgs.ID)
			return syscall.EIO
		}
	} else {
		// string
		_, err := f.client.Set(f.name, f.wb, 0).Result()
		if err != nil {
			fmt.Println("Flush:Set", err, f.name)
			return syscall.EIO
		}
	}

	f.wb = nil
	return nil
}

func (f *redisFile) Attr(ctx context.Context, a *fuse.Attr) error {
	// fill fuse.Attr
	a.Valid = f.attrValidity
	a.Size = f.size
	a.Mode = 0444
	return nil
}

func (f *redisFile) reloadFile(ctx context.Context) error {

	t, err := f.client.Type(f.name).Result()
	if err == redis.Nil {
		return syscall.ENOENT
	}
	if err != nil {
		return syscall.EIO
	}

	var b []byte
	switch t {
	case "string":
		b, err = f.client.Get(f.name).Bytes()
	case "list":
		var values []string
		values, err = f.client.LRange(f.name, 0, -1).Result()
		if err != nil {
			break
		}
		for i, _ := range values {
			b = append(b, []byte(values[i])...)
			if i != len(values)-1 {
				b = append(b, '\n')
			}
		}
	case "stream":
		var resp []redis.XMessage
		resp, err = f.client.XRange(f.name, "-", "+").Result()
		if err != nil {
			break
		}
		b, err = json.Marshal(resp)
	default:
		return syscall.ENOTSUP
	}
	if err == redis.Nil {
		return syscall.ENOENT
	}
	if err != nil {
		return syscall.EIO
	}

	f.rb = b
	f.size = uint64(len(b))

	return nil
}

func (f *redisFile) ReadAll(ctx context.Context) ([]byte, error) {

	if err := f.reloadFile(ctx); err != nil {
		return nil, err
	}

	return f.rb, nil
}
