package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/hashicorp/go-multierror"
	"github.com/lanrat/extsort"
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/cache"
	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/parallel"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

const (
	extsortChannelBufferSize = 1_000
	extsortChunkSize         = 100_000
)

var syncHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source destination

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	01. Sync local folder to s3 bucket
		 > s5cmd {{.HelpName}} folder/ s3://bucket/

	02. Sync S3 bucket to local folder
		 > s5cmd {{.HelpName}} "s3://bucket/*" folder/

	03. Sync S3 bucket objects under prefix to S3 bucket.
		 > s5cmd {{.HelpName}} "s3://sourcebucket/prefix/*" s3://destbucket/

	04. Sync local folder to S3 but delete the files that S3 bucket has but local does not have.
		 > s5cmd {{.HelpName}} --delete folder/ s3://bucket/

	05. Sync S3 bucket to local folder but use size as only comparison criteria.
		 > s5cmd {{.HelpName}} --size-only "s3://bucket/*" folder/

	06. Sync a file to S3 bucket
		 > s5cmd {{.HelpName}} myfile.gz s3://bucket/

	07. Sync matching S3 objects to another bucket
		 > s5cmd {{.HelpName}} "s3://bucket/*.gz" s3://target-bucket/prefix/

	08. Perform KMS Server Side Encryption of the object(s) at the destination
		 > s5cmd {{.HelpName}} --sse aws:kms s3://bucket/object s3://target-bucket/prefix/object

	09. Perform KMS-SSE of the object(s) at the destination using customer managed Customer Master Key (CMK) key id
		 > s5cmd {{.HelpName}} --sse aws:kms --sse-kms-key-id <your-kms-key-id> s3://bucket/object s3://target-bucket/prefix/object

	10. Sync all files to S3 bucket but exclude the ones with txt and gz extension
		 > s5cmd {{.HelpName}} --exclude "*.txt" --exclude "*.gz" dir/ s3://bucket

	11. Sync all files to S3 bucket but include the only ones with txt and gz extension
		 > s5cmd {{.HelpName}} --include "*.txt" --include "*.gz" dir/ s3://bucket
`

func NewSyncCommandFlags() []cli.Flag {
	syncFlags := []cli.Flag{
		&cli.BoolFlag{
			Name:  "delete",
			Usage: "delete objects in destination but not in source",
		},
		&cli.BoolFlag{
			Name:  "size-only",
			Usage: "make size of object only criteria to decide whether an object should be synced",
		},
		&cli.BoolFlag{
			Name:  "exit-on-error",
			Usage: "stops the sync process if an error is received",
		},
		&cli.BoolFlag{
			Name:  "use-cache",
			Usage: "use Redis cache instead of live listing for source and destination",
		},
		&cli.BoolFlag{
			Name:  "since-last-sync",
			Usage: "only consider source objects modified since the last successful sync (requires --use-cache)",
		},
		&cli.BoolFlag{
			Name:  "start-after",
			Usage: "use S3 StartAfter to list only objects with keys after the last synced key (append-only ordered sources, requires --use-cache)",
		},
	}
	sharedFlags := NewSharedFlags()
	return append(syncFlags, sharedFlags...)
}

func NewSyncCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "sync",
		HelpName:           "sync",
		Usage:              "sync objects",
		Flags:              NewSyncCommandFlags(),
		CustomHelpTemplate: syncHelpTemplate,
		Before: func(c *cli.Context) error {
			// sync command share same validation method as copy command
			err := validateCopyCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			return NewSync(c).Run(c)
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}

type ObjectPair struct {
	src, dst *storage.Object
}

// Sync holds sync operation flags and states.
type Sync struct {
	src         string
	dst         string
	op          string
	fullCommand string

	// flags
	delete        bool
	sizeOnly      bool
	exitOnError   bool
	useCache      bool
	sinceLastSync bool
	startAfter    bool
	redisURL      string

	// s3 options
	storageOpts storage.Options

	followSymlinks bool
	storageClass   storage.StorageClass
	raw            bool

	srcRegion string
	dstRegion string
}

// NewSync creates Sync from cli.Context
func NewSync(c *cli.Context) Sync {
	return Sync{
		src:         c.Args().Get(0),
		dst:         c.Args().Get(1),
		op:          c.Command.Name,
		fullCommand: commandFromContext(c),

		// flags
		delete:        c.Bool("delete"),
		sizeOnly:      c.Bool("size-only"),
		exitOnError:   c.Bool("exit-on-error"),
		useCache:      c.Bool("use-cache"),
		sinceLastSync: c.Bool("since-last-sync"),
		startAfter:    c.Bool("start-after"),
		redisURL:      c.String("redis-url"),

		// flags
		followSymlinks: !c.Bool("no-follow-symlinks"),
		storageClass:   storage.StorageClass(c.String("storage-class")),
		raw:            c.Bool("raw"),
		// region settings
		srcRegion:   c.String("source-region"),
		dstRegion:   c.String("destination-region"),
		storageOpts: NewStorageOpts(c),
	}
}

// Run compares files, plans necessary s5cmd commands to execute
// and executes them in order to sync source to destination.
func (s Sync) Run(c *cli.Context) error {
	srcurl, err := url.New(s.src, url.WithRaw(s.raw))
	if err != nil {
		return err
	}

	dsturl, err := url.New(s.dst, url.WithRaw(s.raw))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()

	// Record sync start time before we begin (used for --since-last-sync on next run)
	syncStartTime := time.Now().UTC()

	var sourceObjects, destObjects chan *storage.Object
	if s.useCache {
		redisURL := c.String("redis-url")
		if redisURL == "" {
			redisURL = "redis://localhost:6379"
		}

		// Determine the modifiedAfter filter
		var modifiedAfter time.Time
		if s.sinceLastSync {
			timeClient, err := cache.New(redisURL, 0)
			if err == nil {
				modifiedAfter, _ = timeClient.GetLastSyncTime(ctx, s.src, s.dst)
				timeClient.Close()
			}
		}

		if s.startAfter {
			// StartAfter mode: use S3 StartAfter to only list objects with keys
			// after the last synced key. Zero full scans, minimal S3 pages.
			sourceObjects, destObjects, err = s.getObjectsStartAfter(ctx, cancel, redisURL, srcurl, dsturl)
		} else if s.sinceLastSync && !modifiedAfter.IsZero() {
			// Incremental mode: use time-indexed cache (ZRANGEBYSCORE)
			sourceObjects, destObjects, err = s.getObjectsIncremental(ctx, cancel, redisURL, srcurl, dsturl, modifiedAfter)
		} else {
			sourceObjects, destObjects, err = s.getObjectsFromCache(ctx, redisURL, srcurl, dsturl)
		}
	} else {
		sourceObjects, destObjects, err = s.getSourceAndDestinationObjects(ctx, cancel, srcurl, dsturl)
	}
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	isBatch := srcurl.IsWildcard()
	if !isBatch && !srcurl.IsRemote() {
		sourceClient, err := storage.NewClient(ctx, srcurl, s.storageOpts)
		if err != nil {
			return err
		}

		obj, err := sourceClient.Stat(ctx, srcurl)
		if err != nil {
			return err
		}

		isBatch = obj != nil && obj.Type.IsDir()
	}

	onlySource, onlyDest, commonObjects := compareObjects(sourceObjects, destObjects, isBatch)

	waiter := parallel.NewWaiter()
	var (
		merrorWaiter error
		errDoneCh    = make(chan struct{})
	)

	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			if strings.Contains(err.Error(), "too many open files") {
				fmt.Println(strings.TrimSpace(fdlimitWarning))
				fmt.Printf("ERROR %v\n", err)

				os.Exit(1)
			}
			printError(s.fullCommand, s.op, err)
			merrorWaiter = multierror.Append(merrorWaiter, err)
		}
	}()

	strategy := NewStrategy(s.sizeOnly)
	if s.useCache {
		strategy = &EtagAwareStrategy{Inner: strategy}
	}
	pipeReader, pipeWriter := io.Pipe()

	var cacheClient *cache.Client
	if s.redisURL != "" {
		var err error
		cacheClient, err = cache.New(s.redisURL, 0)
		if err != nil {
			log.Error(log.ErrorMessage{Operation: s.op, Err: fmt.Sprintf("failed to connect to Redis for cache update: %v", err)})
		}
		if cacheClient != nil {
			defer cacheClient.Close()
		}
	}

	go s.planRun(c, onlySource, onlyDest, commonObjects, dsturl, strategy, pipeWriter, isBatch, cacheClient)

	err = NewRun(c, pipeReader).Run(ctx)

	// Record last sync time on success
	if err == nil && s.useCache && s.redisURL != "" {
		tsClient, tsErr := cache.New(s.redisURL, 0)
		if tsErr == nil {
			_ = tsClient.SetLastSyncTime(ctx, s.src, s.dst, syncStartTime)
			tsClient.Close()
		}
	}

	return multierror.Append(err, merrorWaiter).ErrorOrNil()
}

// compareObjects compares source and destination objects. It assumes that
// sourceObjects and destObjects channels are already sorted in ascending order.
// Returns objects those in only source, only destination
// and both.
func compareObjects(sourceObjects, destObjects chan *storage.Object, isSrcBatch bool) (chan *storage.Object, chan *url.URL, chan *ObjectPair) {
	var (
		srcOnly   = make(chan *storage.Object, extsortChannelBufferSize)
		dstOnly   = make(chan *url.URL, extsortChannelBufferSize)
		commonObj = make(chan *ObjectPair, extsortChannelBufferSize)
		srcName   string
		dstName   string
	)

	go func() {
		src, srcOk := <-sourceObjects
		dst, dstOk := <-destObjects

		defer close(srcOnly)
		defer close(dstOnly)
		defer close(commonObj)

		for {
			if srcOk {
				srcName = filepath.ToSlash(src.URL.Relative())
				if !isSrcBatch {
					srcName = src.URL.Base()
				}
			}
			if dstOk {
				dstName = filepath.ToSlash(dst.URL.Relative())
			}

			if srcOk && dstOk {
				if srcName < dstName {
					srcOnly <- src
					src, srcOk = <-sourceObjects
				} else if srcName == dstName { // if there is a match.
					commonObj <- &ObjectPair{src: src, dst: dst}
					src, srcOk = <-sourceObjects
					dst, dstOk = <-destObjects
				} else {
					dstOnly <- dst.URL
					dst, dstOk = <-destObjects
				}
			} else if srcOk {
				srcOnly <- src
				src, srcOk = <-sourceObjects
			} else if dstOk {
				dstOnly <- dst.URL
				dst, dstOk = <-destObjects
			} else /* if !srcOK && !dstOk */ {
				break
			}
		}
	}()

	return srcOnly, dstOnly, commonObj
}

// getSourceAndDestinationObjects returns source and destination objects from
// given URLs. The returned channels gives objects sorted in ascending order
// with respect to their url.Relative path. See also storage.Less.
func (s Sync) getSourceAndDestinationObjects(ctx context.Context, cancel context.CancelFunc, srcurl, dsturl *url.URL) (chan *storage.Object, chan *storage.Object, error) {
	sourceClient, err := storage.NewClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		return nil, nil, err
	}

	destClient, err := storage.NewClient(ctx, dsturl, s.storageOpts)
	if err != nil {
		return nil, nil, err
	}

	// add * to end of destination string, to get all objects recursively.
	var destinationURLPath string
	if strings.HasSuffix(s.dst, "/") {
		destinationURLPath = s.dst + "*"
	} else {
		destinationURLPath = s.dst + "/*"
	}

	destObjectsURL, err := url.New(destinationURLPath)
	if err != nil {
		return nil, nil, err
	}

	var (
		sourceObjects = make(chan *storage.Object, extsortChannelBufferSize)
		destObjects   = make(chan *storage.Object, extsortChannelBufferSize)
	)

	extsortDefaultConfig := extsort.DefaultConfig()
	extsortConfig := &extsort.Config{
		ChunkSize:          extsortChunkSize,
		NumWorkers:         extsortDefaultConfig.NumWorkers,
		ChanBuffSize:       extsortChannelBufferSize,
		SortedChanBuffSize: extsortChannelBufferSize,
	}
	extsortDefaultConfig = nil

	// get source objects.
	go func() {
		defer close(sourceObjects)
		unfilteredSrcObjectChannel := sourceClient.List(ctx, srcurl, s.followSymlinks)
		filteredSrcObjectChannel := make(chan extsort.SortType, extsortChannelBufferSize)

		go func() {
			defer close(filteredSrcObjectChannel)
			// filter and redirect objects
			for st := range unfilteredSrcObjectChannel {
				if st.Err != nil && s.shouldStopSync(st.Err) {
					msg := log.ErrorMessage{
						Err:       cleanupError(st.Err),
						Command:   s.fullCommand,
						Operation: s.op,
					}
					log.Error(msg)
					cancel()
				}
				if s.shouldSkipSrcObject(st, true) {
					continue
				}
				filteredSrcObjectChannel <- *st
			}
		}()

		var (
			sorter        *extsort.SortTypeSorter
			srcOutputChan chan extsort.SortType
		)

		sorter, srcOutputChan, srcErrCh := extsort.New(filteredSrcObjectChannel, storage.FromBytes, storage.Less, extsortConfig)
		sorter.Sort(ctx)

		for srcObject := range srcOutputChan {
			o := srcObject.(storage.Object)
			sourceObjects <- &o
		}

		// read and print the external sort errors
		go func() {
			for err := range srcErrCh {
				printError(s.fullCommand, s.op, err)
			}
		}()
	}()

	// get destination objects.
	go func() {
		defer close(destObjects)
		unfilteredDestObjectsChannel := destClient.List(ctx, destObjectsURL, false)
		filteredDstObjectChannel := make(chan extsort.SortType, extsortChannelBufferSize)

		go func() {
			defer close(filteredDstObjectChannel)
			// filter and redirect objects
			for dt := range unfilteredDestObjectsChannel {
				if dt.Err != nil && s.shouldStopSync(dt.Err) {
					msg := log.ErrorMessage{
						Err:       cleanupError(dt.Err),
						Command:   s.fullCommand,
						Operation: s.op,
					}
					log.Error(msg)
					cancel()
				}
				if s.shouldSkipDstObject(dt, false) {
					continue
				}
				filteredDstObjectChannel <- *dt
			}
		}()

		var (
			dstSorter     *extsort.SortTypeSorter
			dstOutputChan chan extsort.SortType
		)

		dstSorter, dstOutputChan, dstErrCh := extsort.New(filteredDstObjectChannel, storage.FromBytes, storage.Less, extsortConfig)
		dstSorter.Sort(ctx)

		for destObject := range dstOutputChan {
			o := destObject.(storage.Object)
			destObjects <- &o
		}

		// read and print the external sort errors
		go func() {
			for err := range dstErrCh {
				printError(s.fullCommand, s.op, err)
			}
		}()
	}()

	return sourceObjects, destObjects, nil
}

// planRun prepares the commands and writes them to writer 'w'.
func (s Sync) planRun(
	c *cli.Context,
	onlySource chan *storage.Object,
	onlyDest chan *url.URL,
	common chan *ObjectPair,
	dsturl *url.URL,
	strategy SyncStrategy,
	w io.WriteCloser,
	isBatch bool,
	cacheClient *cache.Client,
) {
	defer w.Close()

	defaultFlags := map[string]interface{}{
		"raw": true,
	}

	updateCache := func(srcObj *storage.Object, dstURL *url.URL) {
		if cacheClient == nil || srcObj == nil {
			return
		}
		ctx := c.Context
		modtime := zeroTime
		if srcObj.ModTime != nil {
			modtime = *srcObj.ModTime
		}
		e := cache.Entry{Size: srcObj.Size, ModTime: modtime, Etag: srcObj.Etag}
		entries := map[string]cache.Entry{
			srcObj.URL.Absolute(): e,
			dstURL.Absolute():     e,
		}
		if err := cacheClient.SetPipelined(ctx, entries); err != nil {
			log.Error(log.ErrorMessage{Operation: s.op, Err: fmt.Sprintf("cache update failed: %v", err)})
		}
	}

	// it should wait until both of the child goroutines for onlySource and common channels
	// are completed before closing the WriteCloser w to ensure that all URLs are processed.
	var wg sync.WaitGroup

	// only in source
	wg.Add(1)
	go func() {
		defer wg.Done()
		for srcObj := range onlySource {
			curDestURL := generateDestinationURL(srcObj.URL, dsturl, isBatch)
			updateCache(srcObj, curDestURL)
			command, err := generateCommand(c, "cp", defaultFlags, srcObj.URL, curDestURL)
			if err != nil {
				printDebug(s.op, err, srcObj.URL, curDestURL)
				continue
			}
			fmt.Fprintln(w, command)
		}
	}()

	// both in source and destination
	wg.Add(1)
	go func() {
		defer wg.Done()
		for commonObject := range common {
			sourceObject, destObject := commonObject.src, commonObject.dst
			curSourceURL, curDestURL := sourceObject.URL, destObject.URL
			err := strategy.ShouldSync(sourceObject, destObject)
			if err != nil {
				printDebug(s.op, err, curSourceURL, curDestURL)
				continue
			}
			updateCache(sourceObject, curDestURL)
			command, err := generateCommand(c, "cp", defaultFlags, curSourceURL, curDestURL)
			if err != nil {
				printDebug(s.op, err, curSourceURL, curDestURL)
				continue
			}
			fmt.Fprintln(w, command)
		}
	}()

	// only in destination
	wg.Add(1)
	go func() {
		defer wg.Done()
		if s.delete {
			// unfortunately we need to read them all!
			// or rewrite generateCommand function?
			dstURLs := make([]*url.URL, 0, extsortChunkSize)

			for d := range onlyDest {
				dstURLs = append(dstURLs, d)
			}

			if len(dstURLs) == 0 {
				return
			}

			command, err := generateCommand(c, "rm", defaultFlags, dstURLs...)
			if err != nil {
				printDebug(s.op, err, dstURLs...)
				return
			}
			fmt.Fprintln(w, command)
		} else {
			// we only need  to consume them from the channel so that rest of the objects
			// can be sent to channel.
			for d := range onlyDest {
				_ = d
			}
		}
	}()

	wg.Wait()
}

// generateDestinationURL generates destination url for given
// source url if it would have been in destination.
func generateDestinationURL(srcurl, dsturl *url.URL, isBatch bool) *url.URL {
	objname := srcurl.Base()
	if isBatch {
		objname = srcurl.Relative()
	}

	if dsturl.IsRemote() {
		if dsturl.IsPrefix() || dsturl.IsBucket() {
			return dsturl.Join(objname)
		}
		return dsturl.Clone()

	}

	return dsturl.Join(objname)
}

// getObjectsFromCache returns source and destination objects read from Redis
// sorted in ascending order, matching the contract expected by compareObjects.
func (s Sync) getObjectsFromCache(ctx context.Context, redisURL string, srcurl, dsturl *url.URL) (chan *storage.Object, chan *storage.Object, error) {
	// create two separate clients so each fill goroutine owns its connection
	srcClient, err := cache.New(redisURL, 0)
	if err != nil {
		return nil, nil, err
	}
	dstClient, err := cache.New(redisURL, 0)
	if err != nil {
		srcClient.Close()
		return nil, nil, err
	}

	extsortDefaultConfig := extsort.DefaultConfig()
	extsortConfig := &extsort.Config{
		ChunkSize:          extsortChunkSize,
		NumWorkers:         extsortDefaultConfig.NumWorkers,
		ChanBuffSize:       extsortChannelBufferSize,
		SortedChanBuffSize: extsortChannelBufferSize,
	}

	fill := func(client *cache.Client, prefix string, baseurl *url.URL) chan *storage.Object {
		sortedCh := make(chan *storage.Object, extsortChannelBufferSize)
		go func() {
			defer close(sortedCh)
			defer client.Close()

			// feed raw (unsorted) objects from Redis into extsort via channel-based scan
			unsortedCh := make(chan extsort.SortType, extsortChannelBufferSize)
			resultCh, scanErrCh := client.ScanChan(ctx, prefix)

			go func() {
				defer close(unsortedCh)
				for r := range resultCh {
					u, err := url.New(r.Path, url.WithRaw(s.raw))
					if err != nil {
						continue
					}
					u.SetRelative(baseurl)
					modtime := r.Entry.ModTime
					obj := storage.Object{
						URL:     u,
						Size:    r.Entry.Size,
						ModTime: &modtime,
						Etag:    r.Entry.Etag,
					}
					select {
					case unsortedCh <- obj:
					case <-ctx.Done():
						return
					}
				}
				// drain scan errors
				for err := range scanErrCh {
					printError(s.fullCommand, s.op, err)
				}
			}()

			sorter, outputCh, errCh := extsort.New(unsortedCh, storage.FromBytes, storage.Less, extsortConfig)
			sorter.Sort(ctx)
			for o := range outputCh {
				obj := o.(storage.Object)
				sortedCh <- &obj
			}
			go func() {
				for err := range errCh {
					printError(s.fullCommand, s.op, err)
				}
			}()
		}()
		return sortedCh
	}

	srcPrefix := cache.BucketPrefix(srcurl.Absolute())
	dstPrefix := cache.BucketPrefix(dsturl.Absolute())
	return fill(srcClient, srcPrefix, srcurl), fill(dstClient, dstPrefix, dsturl), nil
}

const incrementalBatchSize = 500

// getObjectsIncremental uses the time-indexed cache to fetch only source objects
// modified after modifiedAfter (O(log N + K) via Redis ZRANGEBYSCORE), then does
// batch point lookups for destination. No S3 API calls, no full cache scan.
func (s Sync) getObjectsIncremental(ctx context.Context, cancel context.CancelFunc, redisURL string, srcurl, dsturl *url.URL, modifiedAfter time.Time) (chan *storage.Object, chan *storage.Object, error) {
	srcClient, err := cache.New(redisURL, 0)
	if err != nil {
		return nil, nil, err
	}
	dstCacheClient, err := cache.New(redisURL, 0)
	if err != nil {
		srcClient.Close()
		return nil, nil, err
	}

	extsortDefaultConfig := extsort.DefaultConfig()
	extsortConfig := &extsort.Config{
		ChunkSize:          extsortChunkSize,
		NumWorkers:         extsortDefaultConfig.NumWorkers,
		ChanBuffSize:       extsortChannelBufferSize,
		SortedChanBuffSize: extsortChannelBufferSize,
	}

	sourceObjects := make(chan *storage.Object, extsortChannelBufferSize)
	destObjects := make(chan *storage.Object, extsortChannelBufferSize)

	go func() {
		defer close(sourceObjects)
		defer close(destObjects)
		defer srcClient.Close()
		defer dstCacheClient.Close()

		// O(log N + K): get only source entries modified after last sync
		srcPrefix := srcurl.Absolute()
		if strings.HasSuffix(srcPrefix, "*") {
			srcPrefix = srcPrefix[:len(srcPrefix)-1]
		}
		if !strings.HasSuffix(srcPrefix, "/") {
			// Find the bucket prefix for the timeline key
			if idx := strings.Index(srcPrefix[len("s3://"):], "/"); idx >= 0 {
				srcPrefix = srcPrefix[:len("s3://")+idx+1]
			}
		}

		modifiedEntries, err := srcClient.ScanModifiedAfter(ctx, srcPrefix, modifiedAfter)
		if err != nil {
			printError(s.fullCommand, s.op, err)
			return
		}

		if len(modifiedEntries) == 0 {
			return
		}

		// Sort the modified source objects
		unsortedSrcCh := make(chan extsort.SortType, len(modifiedEntries))
		go func() {
			defer close(unsortedSrcCh)
			for _, r := range modifiedEntries {
				u, err := url.New(r.Path, url.WithRaw(s.raw))
				if err != nil {
					continue
				}
				u.SetRelative(srcurl)
				modtime := r.Entry.ModTime
				obj := storage.Object{
					URL:     u,
					Size:    r.Entry.Size,
					ModTime: &modtime,
					Etag:    r.Entry.Etag,
				}
				unsortedSrcCh <- obj
			}
		}()

		sorter, srcOutputCh, srcErrCh := extsort.New(unsortedSrcCh, storage.FromBytes, storage.Less, extsortConfig)
		sorter.Sort(ctx)

		var sortedSrc []*storage.Object
		for o := range srcOutputCh {
			obj := o.(storage.Object)
			sortedSrc = append(sortedSrc, &obj)
		}
		go func() {
			for err := range srcErrCh {
				printError(s.fullCommand, s.op, err)
			}
		}()

		// Build destination paths for batch lookup
		dstPrefix := dsturl.Absolute()
		if !strings.HasSuffix(dstPrefix, "/") {
			dstPrefix += "/"
		}

		dstPaths := make([]string, len(sortedSrc))
		for i, obj := range sortedSrc {
			rel := filepath.ToSlash(obj.URL.Relative())
			if dsturl.IsRemote() {
				dstPaths[i] = dstPrefix + rel
			} else {
				dstPaths[i] = filepath.Join(dsturl.Absolute(), rel)
			}
		}

		// Batch lookup destination entries from cache
		dstEntryMap := make(map[string]cache.Entry)
		for i := 0; i < len(dstPaths); i += incrementalBatchSize {
			end := i + incrementalBatchSize
			if end > len(dstPaths) {
				end = len(dstPaths)
			}
			batch, batchErr := dstCacheClient.GetPipelined(ctx, dstPaths[i:end])
			if batchErr != nil {
				printError(s.fullCommand, s.op, batchErr)
				continue
			}
			for k, v := range batch {
				dstEntryMap[k] = v
			}
		}

		// Emit source and destination objects in sorted order
		for i, srcObj := range sortedSrc {
			sourceObjects <- srcObj

			if dstEntry, ok := dstEntryMap[dstPaths[i]]; ok {
				dstURL, err := url.New(dstPaths[i], url.WithRaw(s.raw))
				if err == nil {
					dstURL.SetRelative(dsturl)
					modtime := dstEntry.ModTime
					destObjects <- &storage.Object{
						URL:     dstURL,
						Size:    dstEntry.Size,
						ModTime: &modtime,
						Etag:    dstEntry.Etag,
					}
				}
			}
		}
	}()

	return sourceObjects, destObjects, nil
}

// getObjectsStartAfter uses S3 ListObjectsV2 with StartAfter parameter to only
// list objects whose key is lexicographically after the last synced key.
// For append-only sources with ordered filenames, this means S3 returns ONLY new objects.
// Zero Redis scans, minimal S3 API pages (only pages containing new objects).
func (s Sync) getObjectsStartAfter(ctx context.Context, cancel context.CancelFunc, redisURL string, srcurl, dsturl *url.URL) (chan *storage.Object, chan *storage.Object, error) {
	// Get the last synced key from Redis
	markerClient, err := cache.New(redisURL, 0)
	if err != nil {
		return nil, nil, err
	}
	startAfterKey, _ := markerClient.GetLastStartAfter(ctx, s.src, s.dst)
	markerClient.Close()

	// Create S3 client
	sourceClient, err := storage.NewRemoteClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		return nil, nil, err
	}

	// Create Redis client for destination lookups
	dstCacheClient, err := cache.New(redisURL, 0)
	if err != nil {
		return nil, nil, err
	}

	sourceObjects := make(chan *storage.Object, extsortChannelBufferSize)
	destObjects := make(chan *storage.Object, extsortChannelBufferSize)

	go func() {
		defer close(sourceObjects)
		defer close(destObjects)
		defer dstCacheClient.Close()

		// List from S3 with StartAfter — S3 only returns keys > startAfterKey
		var objCh <-chan *storage.Object
		if startAfterKey != "" {
			objCh = sourceClient.ListStartAfter(ctx, srcurl, startAfterKey)
		} else {
			// First run: list everything
			objCh = sourceClient.List(ctx, srcurl, s.followSymlinks)
		}

		// Collect new source objects (already sorted by S3 key order)
		var newObjects []*storage.Object
		var lastKey string
		for obj := range objCh {
			if obj.Err != nil {
				if s.shouldStopSync(obj.Err) {
					printError(s.fullCommand, s.op, obj.Err)
					cancel()
					return
				}
				continue
			}
			if s.shouldSkipSrcObject(obj, false) {
				continue
			}
			newObjects = append(newObjects, obj)
			if obj.URL.Path > lastKey {
				lastKey = obj.URL.Path
			}
		}

		if len(newObjects) == 0 {
			return
		}

		// Save the last key for next run
		if lastKey != "" && s.redisURL != "" {
			mkClient, err := cache.New(s.redisURL, 0)
			if err == nil {
				_ = mkClient.SetLastStartAfter(ctx, s.src, s.dst, lastKey)
				mkClient.Close()
			}
		}

		// Batch lookup destination entries from cache
		dstPrefix := dsturl.Absolute()
		if !strings.HasSuffix(dstPrefix, "/") {
			dstPrefix += "/"
		}

		dstPaths := make([]string, len(newObjects))
		for i, obj := range newObjects {
			rel := filepath.ToSlash(obj.URL.Relative())
			if dsturl.IsRemote() {
				dstPaths[i] = dstPrefix + rel
			} else {
				dstPaths[i] = filepath.Join(dsturl.Absolute(), rel)
			}
		}

		dstEntryMap := make(map[string]cache.Entry)
		for i := 0; i < len(dstPaths); i += incrementalBatchSize {
			end := i + incrementalBatchSize
			if end > len(dstPaths) {
				end = len(dstPaths)
			}
			batch, batchErr := dstCacheClient.GetPipelined(ctx, dstPaths[i:end])
			if batchErr != nil {
				printError(s.fullCommand, s.op, batchErr)
				continue
			}
			for k, v := range batch {
				dstEntryMap[k] = v
			}
		}

		// Emit objects (S3 ListObjects already returns in sorted key order)
		for i, srcObj := range newObjects {
			sourceObjects <- srcObj

			if dstEntry, ok := dstEntryMap[dstPaths[i]]; ok {
				dstURL, err := url.New(dstPaths[i], url.WithRaw(s.raw))
				if err == nil {
					dstURL.SetRelative(dsturl)
					modtime := dstEntry.ModTime
					destObjects <- &storage.Object{
						URL:     dstURL,
						Size:    dstEntry.Size,
						ModTime: &modtime,
						Etag:    dstEntry.Etag,
					}
				}
			}
		}
	}()

	return sourceObjects, destObjects, nil
}

// shouldSkipObject checks is object should be skipped.
func (s Sync) shouldSkipSrcObject(object *storage.Object, verbose bool) bool {
	if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
		return true
	}

	if err := object.Err; err != nil {
		if verbose {
			printError(s.fullCommand, s.op, err)
		}
		return true
	}

	if object.StorageClass.IsGlacier() {
		if verbose {
			err := fmt.Errorf("object '%v' is on Glacier storage", object)
			printError(s.fullCommand, s.op, err)
		}
		return true
	}
	return false
}

func (s Sync) shouldSkipDstObject(object *storage.Object, verbose bool) bool {
	if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
		return true
	}

	if err := object.Err; err != nil {
		if verbose {
			printError(s.fullCommand, s.op, err)
		}
		return true
	}

	return false
}

// shouldStopSync determines whether a sync process should be stopped or not.
func (s Sync) shouldStopSync(err error) bool {
	if err == storage.ErrNoObjectFound {
		return false
	}
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case "AccessDenied", "NoSuchBucket":
			return true
		}
	}
	return s.exitOnError
}
