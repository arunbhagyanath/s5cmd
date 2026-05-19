package command

import (
	"context"
	"sync"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/cache"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

const (
	cacheBuildBatchSize = 1000
	numFlushWorkers     = 8
)

var zeroTime = time.Time{}

func NewCacheBuildCommand() *cli.Command {
	return &cli.Command{
		Name:      "cache-build",
		HelpName:  "cache-build",
		Usage:     "build Redis cache for a source or destination path",
		ArgsUsage: "path (local dir or s3://bucket/prefix)",
		Action:    runCacheBuild,
	}
}

func runCacheBuild(c *cli.Context) error {
	redisURL := c.String("redis-url")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	client, err := cache.New(redisURL, 0)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := c.Context
	srcurl, err := url.New(c.Args().First())
	if err != nil {
		return err
	}

	storageClient, err := storage.NewClient(ctx, srcurl, NewStorageOpts(c))
	if err != nil {
		return err
	}

	return parallelCacheBuild(ctx, client, storageClient, srcurl)
}

func parallelCacheBuild(ctx context.Context, client *cache.Client, storageClient storage.Storage, srcurl *url.URL) error {
	batchCh := make(chan map[string]cache.Entry, numFlushWorkers*2)

	errCh := make(chan error, numFlushWorkers)
	var wg sync.WaitGroup
	for i := 0; i < numFlushWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				if err := client.SetPipelined(ctx, batch); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	go func() {
		defer close(batchCh)
		batch := make(map[string]cache.Entry, cacheBuildBatchSize)
		for obj := range storageClient.List(ctx, srcurl, true) {
			if obj.Err != nil || obj.Type.IsDir() {
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
	return <-errCh
}
