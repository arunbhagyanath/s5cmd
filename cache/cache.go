package cache

import (
	"context"
	"strconv"
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

func (c *Client) SetPipelined(ctx context.Context, entries map[string]Entry) error {
	pipe := c.rdb.Pipeline()
	for path, e := range entries {
		pipe.HSet(ctx, key(path),
			"size", e.Size,
			"modtime", e.ModTime.UTC().Format(time.RFC3339Nano),
			"etag", e.Etag,
		)
		if c.ttl > 0 {
			pipe.Expire(ctx, key(path), c.ttl)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Scan iterates all cached keys under the given URL prefix, calling fn for each.
// HGetAll calls within each SCAN page are parallelized.
func (c *Client) Scan(ctx context.Context, urlPrefix string, fn func(path string, e Entry)) error {
	pattern := keyPrefix + urlPrefix + "*"
	var cursor uint64
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			pipe := c.rdb.Pipeline()
			cmds := make([]*redis.MapStringStringCmd, len(keys))
			for i, k := range keys {
				cmds[i] = pipe.HGetAll(ctx, k)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}

			var wg sync.WaitGroup
			var mu sync.Mutex
			for i, k := range keys {
				i, k := i, k
				wg.Add(1)
				go func() {
					defer wg.Done()
					vals := cmds[i].Val()
					if len(vals) == 0 {
						return
					}
					size, _ := strconv.ParseInt(vals["size"], 10, 64)
					modtime, _ := time.Parse(time.RFC3339Nano, vals["modtime"])
					path := k[len(keyPrefix):]
					mu.Lock()
					fn(path, Entry{Size: size, ModTime: modtime, Etag: vals["etag"]})
					mu.Unlock()
				}()
			}
			wg.Wait()
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}
