package cache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix   = "s5cmd:cache:"
	indexPrefix = "s5cmd:index:"
	indexShards = 1000 // number of index shards per prefix
)

type Entry struct {
	Size    int64
	ModTime time.Time
	Etag    string
}

type Client struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(redisURL string, ttl time.Duration) (*Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return &Client{rdb: redis.NewClient(opts), ttl: ttl}, nil
}

func (c *Client) Close() error { return c.rdb.Close() }

func key(path string) string { return keyPrefix + path }

// shardKey returns the index set key for a given path.
// Shards by FNV hash so 40M keys spread across 1000 sets (~40K members each).
// e.g. "s5cmd:index:s3://bucket/:42"
func shardKey(prefix, path string) string {
	h := fnv32(path)
	return fmt.Sprintf("%s%s:%d", indexPrefix, prefix, h%indexShards)
}

// bucketPrefix extracts the top-level prefix from an absolute path.
// "s3://bucket/blob-data#123" → "s3://bucket/"
// "/data/blobs/blob-data#123" → "/data/blobs/"
func bucketPrefix(path string) string {
	if strings.HasPrefix(path, "s3://") {
		rest := path[len("s3://"):]
		if idx := strings.Index(rest, "/"); idx >= 0 {
			return "s3://" + rest[:idx+1]
		}
		return path + "/"
	}
	trimmed := strings.TrimRight(path, "/")
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 {
		return trimmed[:idx+1]
	}
	return "/"
}

func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func entryToString(e Entry) string {
	return fmt.Sprintf("%d|%s|%s", e.Size, e.ModTime.UTC().Format(time.RFC3339Nano), e.Etag)
}

func entryFromString(s string) Entry {
	parts := strings.SplitN(s, "|", 3)
	if len(parts) != 3 {
		return Entry{}
	}
	size, _ := strconv.ParseInt(parts[0], 10, 64)
	modtime, _ := time.Parse(time.RFC3339Nano, parts[1])
	return Entry{Size: size, ModTime: modtime, Etag: parts[2]}
}

func (c *Client) SetPipelined(ctx context.Context, entries map[string]Entry) error {
	pipe := c.rdb.Pipeline()
	for path, e := range entries {
		pipe.Set(ctx, key(path), entryToString(e), c.ttl)
		pipe.SAdd(ctx, shardKey(bucketPrefix(path), path), path)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) SetIfAbsentPipelined(ctx context.Context, entries map[string]Entry) error {
	pipe := c.rdb.Pipeline()
	for path, e := range entries {
		pipe.SetNX(ctx, key(path), entryToString(e), c.ttl)
		pipe.SAdd(ctx, shardKey(bucketPrefix(path), path), path)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) Set(ctx context.Context, path string, e Entry) error {
	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, key(path), entryToString(e), c.ttl)
	pipe.SAdd(ctx, shardKey(bucketPrefix(path), path), path)
	_, err := pipe.Exec(ctx)
	return err
}

// ScanChan is like Scan but sends results through a channel instead of a callback,
// avoiding mutex contention when shards run in parallel.
func (c *Client) ScanChan(ctx context.Context, urlPrefix string) (<-chan ScanResult, <-chan error) {
	resultCh := make(chan ScanResult, 4096)
	errCh := make(chan error, 1)

	go func() {
		defer close(resultCh)
		defer close(errCh)

		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error

		for shard := 0; shard < indexShards; shard++ {
			shard := shard
			wg.Add(1)
			go func() {
				defer wg.Done()
				idxKey := fmt.Sprintf("%s%s:%d", indexPrefix, urlPrefix, shard)
				var cursor uint64
				for {
					members, next, err := c.rdb.SScan(ctx, idxKey, cursor, "*", 1000).Result()
					if err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
						return
					}
					if len(members) > 0 {
						pipe := c.rdb.Pipeline()
						cmds := make([]*redis.StringCmd, len(members))
						for i, m := range members {
							cmds[i] = pipe.Get(ctx, key(m))
						}
						_, pipeErr := pipe.Exec(ctx)
						if pipeErr != nil && pipeErr != redis.Nil {
							mu.Lock()
							if firstErr == nil {
								firstErr = pipeErr
							}
							mu.Unlock()
							return
						}
						for i, m := range members {
							val, err := cmds[i].Result()
							if err != nil {
								continue
							}
							select {
							case resultCh <- ScanResult{Path: m, Entry: entryFromString(val)}:
							case <-ctx.Done():
								return
							}
						}
					}
					cursor = next
					if cursor == 0 {
						break
					}
				}
			}()
		}

		wg.Wait()
		if firstErr != nil {
			errCh <- firstErr
		}
	}()

	return resultCh, errCh
}

// ScanResult holds a single cache entry returned by ScanChan.
type ScanResult struct {
	Path  string
	Entry Entry
}

// Scan iterates all cached keys under urlPrefix using sharded index sets.
// All 1000 shards are scanned in parallel, each shard has ~40K members
// instead of one sequential scan over 40M members.
func (c *Client) Scan(ctx context.Context, urlPrefix string, fn func(path string, e Entry)) error {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
	)

	for shard := 0; shard < indexShards; shard++ {
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			idxKey := fmt.Sprintf("%s%s:%d", indexPrefix, urlPrefix, shard)
			var cursor uint64
			for {
				members, next, err := c.rdb.SScan(ctx, idxKey, cursor, "*", 1000).Result()
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("shard %d scan failed: %w", shard, err)
					}
					mu.Unlock()
					return
				}
				if len(members) > 0 {
					pipe := c.rdb.Pipeline()
					cmds := make([]*redis.StringCmd, len(members))
					for i, m := range members {
						cmds[i] = pipe.Get(ctx, key(m))
					}
					_, pipeErr := pipe.Exec(ctx)
					// redis.Nil is expected for missing keys in pipeline
					if pipeErr != nil && pipeErr != redis.Nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("shard %d pipeline failed: %w", shard, pipeErr)
						}
						mu.Unlock()
						return
					}
					for i, m := range members {
						val, err := cmds[i].Result()
						if err != nil {
							continue
						}
						e := entryFromString(val)
						fn(m, e)
					}
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}()
	}

	wg.Wait()
	return firstErr
}
