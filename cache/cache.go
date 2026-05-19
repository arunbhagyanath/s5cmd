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

const keyPrefix = "s5cmd:cache:"

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
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Scan iterates all cached keys under the given URL prefix, calling fn for each.
// GET calls within each SCAN page are parallelized.
func (c *Client) Scan(ctx context.Context, urlPrefix string, fn func(path string, e Entry)) error {
	pattern := keyPrefix + urlPrefix + "*"
	var (
		cursor uint64
		pages  int
		total  int
	)
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return fmt.Errorf("cache scan failed at cursor %d after %d objects: %w", cursor, total, err)
		}
		if len(keys) > 0 {
			pages++
			pipe := c.rdb.Pipeline()
			cmds := make([]*redis.StringCmd, len(keys))
			for i, k := range keys {
				cmds[i] = pipe.Get(ctx, k)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("cache scan pipeline failed on page %d: %w", pages, err)
			}

			var wg sync.WaitGroup
			var mu sync.Mutex
			for i, k := range keys {
				i, k := i, k
				wg.Add(1)
				go func() {
					defer wg.Done()
					val, err := cmds[i].Result()
					if err != nil {
						return
					}
					e := entryFromString(val)
					path := k[len(keyPrefix):]
					mu.Lock()
					fn(path, e)
					mu.Unlock()
				}()
			}
			wg.Wait()
			total += len(keys)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}
