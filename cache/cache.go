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
	keyPrefix      = "s5cmd:cache:"
	indexPrefix    = "s5cmd:index:"
	timelinePrefix = "s5cmd:timeline:"
	indexShards    = 1000 // number of index shards per prefix
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

// LastStartAfterKey returns the Redis key for storing the last seen object key.
func LastStartAfterKey(src, dst string) string {
	return keyPrefix + "start-after:" + src + "->" + dst
}

// LastSyncKey returns the Redis key used to store the last sync timestamp.
func LastSyncKey(src, dst string) string {
	return keyPrefix + "last-sync:" + src + "->" + dst
}

// GetLastStartAfter retrieves the last seen object key for a src->dst pair.
func (c *Client) GetLastStartAfter(ctx context.Context, src, dst string) (string, error) {
	val, err := c.rdb.Get(ctx, LastStartAfterKey(src, dst)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

// SetLastStartAfter records the last seen object key for a src->dst pair.
func (c *Client) SetLastStartAfter(ctx context.Context, src, dst, lastKey string) error {
	return c.rdb.Set(ctx, LastStartAfterKey(src, dst), lastKey, 0).Err()
}

// GetLastSyncTime retrieves the last successful sync timestamp for a src->dst pair.
func (c *Client) GetLastSyncTime(ctx context.Context, src, dst string) (time.Time, error) {
	val, err := c.rdb.Get(ctx, LastSyncKey(src, dst)).Result()
	if err == redis.Nil {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, val)
}

// SetLastSyncTime records the sync timestamp for a src->dst pair.
func (c *Client) SetLastSyncTime(ctx context.Context, src, dst string, t time.Time) error {
	return c.rdb.Set(ctx, LastSyncKey(src, dst), t.UTC().Format(time.RFC3339Nano), 0).Err()
}

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
		// Add to time-indexed sorted set (score = unix nano of ModTime)
		tlKey := timelinePrefix + bucketPrefix(path)
		pipe.ZAdd(ctx, tlKey, redis.Z{Score: float64(e.ModTime.UnixNano()), Member: path})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) SetIfAbsentPipelined(ctx context.Context, entries map[string]Entry) error {
	pipe := c.rdb.Pipeline()
	for path, e := range entries {
		pipe.SetNX(ctx, key(path), entryToString(e), c.ttl)
		pipe.SAdd(ctx, shardKey(bucketPrefix(path), path), path)
		tlKey := timelinePrefix + bucketPrefix(path)
		pipe.ZAdd(ctx, tlKey, redis.Z{Score: float64(e.ModTime.UnixNano()), Member: path})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) Set(ctx context.Context, path string, e Entry) error {
	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, key(path), entryToString(e), c.ttl)
	pipe.SAdd(ctx, shardKey(bucketPrefix(path), path), path)
	tlKey := timelinePrefix + bucketPrefix(path)
	pipe.ZAdd(ctx, tlKey, redis.Z{Score: float64(e.ModTime.UnixNano()), Member: path})
	_, err := pipe.Exec(ctx)
	return err
}

// Get retrieves a single cache entry by path. Returns false if not found.
func (c *Client) Get(ctx context.Context, path string) (Entry, bool) {
	val, err := c.rdb.Get(ctx, key(path)).Result()
	if err != nil {
		return Entry{}, false
	}
	return entryFromString(val), true
}

// GetPipelined retrieves multiple cache entries by path in a single round-trip.
// Returns a map of path -> Entry for entries that exist.
func (c *Client) GetPipelined(ctx context.Context, paths []string) (map[string]Entry, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(paths))
	for i, p := range paths {
		cmds[i] = pipe.Get(ctx, key(p))
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	result := make(map[string]Entry, len(paths))
	for i, p := range paths {
		val, err := cmds[i].Result()
		if err != nil {
			continue
		}
		result[p] = entryFromString(val)
	}
	return result, nil
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

// ScanModifiedAfter returns entries under urlPrefix with ModTime > after.
// Uses a Redis Sorted Set (ZRANGEBYSCORE) — O(log N + K) where K = results.
// No full scan of all entries needed.
func (c *Client) ScanModifiedAfter(ctx context.Context, urlPrefix string, after time.Time) ([]ScanResult, error) {
	tlKey := timelinePrefix + urlPrefix
	minScore := fmt.Sprintf("(%d", after.UnixNano()) // exclusive
	maxScore := "+inf"

	// Get all paths with score > after
	paths, err := c.rdb.ZRangeByScore(ctx, tlKey, &redis.ZRangeBy{
		Min: minScore,
		Max: maxScore,
	}).Result()
	if err != nil {
		return nil, err
	}

	if len(paths) == 0 {
		return nil, nil
	}

	// Batch GET the entry values
	results := make([]ScanResult, 0, len(paths))
	for i := 0; i < len(paths); i += 500 {
		end := i + 500
		if end > len(paths) {
			end = len(paths)
		}
		pipe := c.rdb.Pipeline()
		cmds := make([]*redis.StringCmd, end-i)
		for j, p := range paths[i:end] {
			cmds[j] = pipe.Get(ctx, key(p))
		}
		_, pipeErr := pipe.Exec(ctx)
		if pipeErr != nil && pipeErr != redis.Nil {
			return nil, pipeErr
		}
		for j, p := range paths[i:end] {
			val, err := cmds[j].Result()
			if err != nil {
				continue
			}
			results = append(results, ScanResult{Path: p, Entry: entryFromString(val)})
		}
	}

	return results, nil
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
