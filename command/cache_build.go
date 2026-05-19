package command

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/cache"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

const (
	cacheBuildBatchSize    = 1000
	defaultNumFlushWorkers = 8
)

var zeroTime = time.Time{}

func NewCacheBuildCommand() *cli.Command {
	return &cli.Command{
		Name:      "cache-build",
		HelpName:  "cache-build",
		Usage:     "build Redis cache for a source or destination path",
		ArgsUsage: "path (local dir or s3://bucket/prefix)",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "cache-workers",
				Value: defaultNumFlushWorkers,
				Usage: "number of parallel workers flushing batches to Redis",
			},
			&cli.BoolFlag{
				Name:  "resume",
				Usage: "skip objects already present in cache, allowing interrupted builds to continue",
			},
		},
		Action: runCacheBuild,
	}
}

func runCacheBuild(c *cli.Context) error {
	redisURL := c.String("redis-url")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	client, err := cache.New(redisURL, 0)
	if err != nil {
		log.Error(log.ErrorMessage{Operation: "cache-build", Err: fmt.Sprintf("failed to connect to Redis: %v", err)})
		return err
	}
	defer client.Close()

	ctx := c.Context
	srcurl, err := url.New(c.Args().First())
	if err != nil {
		log.Error(log.ErrorMessage{Operation: "cache-build", Err: fmt.Sprintf("invalid path: %v", err)})
		return err
	}

	storageClient, err := storage.NewClient(ctx, srcurl, NewStorageOpts(c))
	if err != nil {
		log.Error(log.ErrorMessage{Operation: "cache-build", Err: fmt.Sprintf("failed to create storage client: %v", err)})
		return err
	}

	log.Info(log.InfoMessage{Operation: "cache-build", Source: srcurl})

	count, err := parallelCacheBuild(ctx, client, storageClient, srcurl, c.Int("cache-workers"), c.Bool("resume"))
	if err != nil {
		log.Error(log.ErrorMessage{Operation: "cache-build", Err: fmt.Sprintf("cache build failed after %d objects: %v", count, err)})
		return err
	}

	log.Info(cacheBuildDoneMessage{Source: srcurl.Absolute(), Count: count})
	return nil
}

func parallelCacheBuild(ctx context.Context, client *cache.Client, storageClient storage.Storage, srcurl *url.URL, workers int, resume bool) (int64, error) {
	batchCh := make(chan map[string]cache.Entry, workers*2)

	var total atomic.Int64
	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				var err error
				if resume {
					err = client.SetIfAbsentPipelined(ctx, batch)
				} else {
					err = client.SetPipelined(ctx, batch)
				}
				if err != nil {
					log.Error(log.ErrorMessage{Operation: "cache-build", Err: fmt.Sprintf("redis pipeline flush failed: %v", err)})
					errCh <- err
					return
				}
				n := int64(len(batch))
				total.Add(n)
				log.Debug(cacheBuildProgressMessage{Count: total.Load(), BatchSize: n})
			}
		}()
	}

	go func() {
		defer close(batchCh)
		batch := make(map[string]cache.Entry, cacheBuildBatchSize)
		for obj := range storageClient.List(ctx, srcurl, true) {
			if obj.Err != nil {
				log.Error(log.ErrorMessage{Operation: "cache-build", Err: fmt.Sprintf("listing error: %v", obj.Err)})
				continue
			}
			if obj.Type.IsDir() {
				continue
			}
			modtime := zeroTime
			if obj.ModTime != nil {
				modtime = *obj.ModTime
			}
			batch[obj.URL.Absolute()] = cache.Entry{
				Size:    obj.Size,
				ModTime: modtime,
				Etag:    obj.Etag,
			}
			log.Debug(cacheBuildObjectMessage{Path: obj.URL.Absolute(), Size: obj.Size})
			if len(batch) >= cacheBuildBatchSize {
				batchCh <- batch
				batch = make(map[string]cache.Entry, cacheBuildBatchSize)
			}
		}
		if len(batch) > 0 {
			batchCh <- batch
		}
	}()

	wg.Wait()
	close(errCh)
	return total.Load(), <-errCh
}

type cacheBuildDoneMessage struct {
	Source string `json:"source"`
	Count  int64  `json:"count"`
}

func (m cacheBuildDoneMessage) String() string {
	return fmt.Sprintf("cache-build finished: %d objects cached from %s", m.Count, m.Source)
}

func (m cacheBuildDoneMessage) JSON() string {
	return fmt.Sprintf(`{"operation":"cache-build","source":%q,"count":%d,"success":true}`, m.Source, m.Count)
}

type cacheBuildProgressMessage struct {
	Count     int64 `json:"total"`
	BatchSize int64 `json:"batch_size"`
}

func (m cacheBuildProgressMessage) String() string {
	return fmt.Sprintf("cache-build progress: %d objects cached (batch: %d)", m.Count, m.BatchSize)
}

func (m cacheBuildProgressMessage) JSON() string {
	return fmt.Sprintf(`{"operation":"cache-build","total":%d,"batch_size":%d}`, m.Count, m.BatchSize)
}

type cacheBuildObjectMessage struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func (m cacheBuildObjectMessage) String() string {
	return fmt.Sprintf("cache-build index: %s (%d bytes)", m.Path, m.Size)
}

func (m cacheBuildObjectMessage) JSON() string {
	return fmt.Sprintf(`{"operation":"cache-build","path":%q,"size":%d}`, m.Path, m.Size)
}
