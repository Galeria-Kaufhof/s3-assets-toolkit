package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/urfave/cli.v1"
	"math"
	"net/url"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type Opts struct {
	TargetBucket string `short: "t", long: "target-bucket"`

	InPlace    bool   `long: "in-place"`
	FromBucket string `long: "from-bucket"`
	// TODO validation - Please provide the source of the objects. Either --in-place or --from-bucket must be specified. Or use InPlace as the default for FromBucket
	SetCacheControl string `long: "set-cache-control" default:"max-age=31536000,public"`
	// cache for one year - so almost forever
	SetCacheControlForever string `long: "set-cache-control-forever"`
}

var opts Opts

// modify aws s3 copy args in place
func args_src_copy_from(key string, opts Opts, input *s3.CopyObjectInput) {
	input.SetCopySource(opts.FromBucket + key)
}

// modify aws s3 copy args in place,
// set cache-control
func args_src_cache_control(opts Opts, input *s3.CopyObjectInput) {
}

// modify aws s3 copy args in place,
// gather existing object meta data, especially content type
func args_src_existing_meta(opts Opts, input *s3.CopyObjectInput) {
}

func copy(object string, opts Opts) {
	prepareCopy(object, opts)
	// svc := s3.New(session.New())
}

func prepareCopy(key string, opts Opts) {
	/*
		object := load_existing_metadata(key)
		input := &s3.CopyObjectInput{
			Bucket: aws.String(opts.TargetBucket),
			Key:    aws.String(key),
		}
		args_src_copy_from(key, opts, input)
		args_src_cache_control(object, opts, input)
		args_src_existing_meta(object, opts, input)
	*/
}

func listObjectsFromStdin(names chan<- string, bucketname string, context CopyContext) {
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		names <- input.Text()
	}
}

func listObjectsToCopy(names chan<- string, bucketname string, context *CopyContext) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketname),
		MaxKeys: aws.Int64(20),
	}

	err := context.s3svc.ListObjectsV2Pages(input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, item := range page.Contents {
				names <- *item.Key
			}
			// stop pumping names once we have copied enough
			return context.copiedObjects < context.maxObjectsToCopy
		})
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%s", err))
	}
}

func main() {

	app := cli.NewApp()
	app.Usage = "Set Cache-Control header for all objects in a s3 bucket. Optionally copies objects from another bucket."
	app.Version = "0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "target-bucket, t",
			Usage: "where changes will happen: objects added or meta-data changed",
		},
		cli.StringFlag{
			Name:  "from-bucket, f",
			Usage: "if omitted, use in-place-copy (target-bucket=from-bucket)",
		},
		cli.StringFlag{
			Name:  "cache-control, c",
			Value: "max-age=31536000,public",
			Usage: "by default cache for one year",
		},
		cli.StringFlag{
			Name:  "exclude-pictures, e",
			Usage: "do not process picture object which names match regex",
		},
		cli.IntFlag{
			Name:  "first-n, n",
			Value: math.MaxInt64,
			Usage: "stop copy/process roughly after first n entries; skipped\n\tand ignored do not count",
		},
	}
	app.Action = func(c *cli.Context) error {
		context, _ := prepareContextFromCli(c)

		parallelity := 200 // set well below the typical ulimit of 1024
		// to avoid "socket: too many open files".
		// Also fits AWS API limits, avoid "503 SlowDown: Please reduce your request rate."
		names := make(chan string)
		context.wg.Add(parallelity)
		for gr := 1; gr <= parallelity; gr++ {
			go cpworker(&context, names)
		}

		listObjectsToCopy(names, context.from, &context)
		close(names)
		context.wg.Wait()
		fmt.Printf("\nDone.\n")
		return nil
	}
	app.Run(os.Args)
}

func CheckPublicCommentTmp() {
}

/* CopyContext defines context for running concurrent copy operations and remembers the progress */
type CopyContext struct {
	s3svc    *s3.S3
	target   string
	from     string
	newvalue string
	exclude  regexp.Regexp

	copiedObjects    int64
	maxObjectsToCopy int64
	processedObjects int64 // including ignored and skipped
	copiedBytes      int64
	expectedObjects  int64
	start            time.Time
	statusLineMutex  sync.Mutex
	lastStatusShown  float64

	wg sync.WaitGroup
}

func prepareContext() (CopyContext, error) {
	// Session with the new library
	sess, err := session.NewSession() /*&aws.Config{
		Region: aws.String("eu-central-1")},
	)*/
	if err != nil {
		panic(fmt.Sprintf("Can not create AWS SDK session %s", err))
	}

	if len(os.Args) != 3 {
		panic("Please provide bucket name and desired Cache-Control setting")
	}
	return CopyContext{
		s3svc:           s3.New(sess),
		target:          os.Args[1],
		expectedObjects: 3867874,
		newvalue:        os.Args[2],
		start:           time.Now(),
	}, nil
}

func prepareContextFromCli(c *cli.Context) (CopyContext, error) {
	// Session with the new library
	sess, err := session.NewSession() /*&aws.Config{
		Region: aws.String("eu-central-1")},
	)*/
	if err != nil {
		panic(fmt.Sprintf("Can not create AWS SDK session %s", err))
	}

	target := c.GlobalString("target-bucket")
	if target == "" {
		cli.ShowAppHelp(c)
		return CopyContext{}, cli.NewExitError("\n\nError: --target-bucket is a required flag\n", 1)
	}

	from := c.GlobalString("from-bucket")
	if from == "" {
		from = target
	}

	fmt.Printf("Copying   to %s\nCopying from %s\n", target, from)

	exclude_pattern := c.GlobalString("exclude-pictures")
	if exclude_pattern == "" {
		exclude_pattern = "^some-pattern-which-would-never-match$"
	}
	return CopyContext{
		s3svc:            s3.New(sess),
		target:           target,
		from:             from,
		expectedObjects:  0,
		maxObjectsToCopy: c.GlobalInt64("first-n"),
		newvalue:         c.GlobalString("cache-control"),
		exclude:          *regexp.MustCompile(exclude_pattern),
		start:            time.Now(),
	}, nil
}

func cpworker(context *CopyContext, names <-chan string) {
	for {
		name, more := <-names
		if more {
			// fmt.Printf("Starting copy %v\n", name)
			if err := cp(context, name); err != nil {
				os.Stderr.WriteString(fmt.Sprintf("==> Failed processing '%s': %v\n", name, err))
				filename := "error_keys.txt"
				os.Stderr.WriteString(fmt.Sprintf("Adding name to '%s' for later processing or reference", filename))
				f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
				if err != nil {
					os.Stderr.WriteString(fmt.Sprintf("Could not write to 'TODO' file '%s'. Error: %v\n", filename, err))
					panic(err)
				}
				fmt.Fprintln(f, name)
				f.Close()
			}
		} else {
			// fmt.Printf("\nNo more data in names channel.\n")
			context.wg.Done()
			return
		}
	}
}

func IsPicture(meta *s3.HeadObjectOutput) bool {
	switch *meta.ContentType {
	case
		"image/jpeg",
		"image/png":
		return true
	default:
		return false
	}
}

func cp(context *CopyContext, name string) error {
	//fmt.Println(context.target)
	//fmt.Println(url.PathEscape(name))
	// key := aws.String(url.PathEscape(name)),
	key := name
	from, err := context.s3svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(context.from),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("\naws sdk Head for `%s` failed: \n%T\n%v\n", key, err, err)
	}

	contenttype := from.ContentType
	oldcachecontrol := from.CacheControl

	var status string
	// I - ignore due pattern
	// . - skip
	// X - type was not set, set to image/png
	// j - was image/jpeg; adjusted CacheControl
	// g - was image/png; adjusted CacheControl
	// P - pdf file; adjusted CacheControl
	// Y - other file type; adjusted CacheControl
	if context.exclude.MatchString(name) && IsPicture(from) {
		status = "I"
	} else if oldcachecontrol != nil && *oldcachecontrol == context.newvalue {
		status = "."
	} else if context.copiedObjects > context.maxObjectsToCopy {
		status = ","
	} else {
		if contenttype == nil {
			status = "X"
			contenttype = aws.String("image/png")
		} else {
			// contenttype = contenttypes[0] // theoretically, there can be multiple HTTP headers with the same key
			// but lets assume, there is at most one
			if *contenttype == "image/png" {
				status = "g"
			} else if *contenttype == "image/jpeg" {
				status = "j"
			} else if *contenttype == "application/pdf" {
				status = "P"
			} else {
				status = "Y"
			}
		}

		src := fmt.Sprintf("%s/%s", context.from, url.PathEscape(name))
		inp := s3.CopyObjectInput{
			Bucket:            aws.String(context.target),
			CopySource:        &src,
			Key:               &name,
			CacheControl:      &context.newvalue,
			ContentType:       contenttype,
			MetadataDirective: aws.String("REPLACE"),
		}
		_, err := context.s3svc.CopyObject(&inp)
		if err != nil {
			return fmt.Errorf("Failed changing (inplace-copying) object: %v", err)
		}
		atomic.AddInt64(&context.copiedObjects, 1)
	}
	fmt.Print(status)

	atomic.AddInt64(&context.processedObjects, 1)
	sec := time.Since(context.start).Seconds()
	o_s := float64(context.processedObjects) / sec
	expectedDuration := time.Duration(int(float64(context.expectedObjects-context.processedObjects)/o_s)) * time.Second
	days := int(expectedDuration.Hours() / 24)
	andHours := expectedDuration.Hours() - float64(days)*24
	eta := fmt.Sprintf("%dd %.1fh", days, andHours)
	if days == 0 {
		hours := int(expectedDuration.Minutes() / 60)
		andMinutes := expectedDuration.Minutes() - float64(hours)*60
		eta = fmt.Sprintf("%dh %.1fm", hours, andMinutes)
	}
	if context.expectedObjects < context.processedObjects {
		eta = "-"
	}

	const everySeconds = 10
	show := false
	context.statusLineMutex.Lock()
	if context.lastStatusShown < sec-everySeconds {
		show = true
		context.lastStatusShown = sec
	}
	context.statusLineMutex.Unlock()

	if show {
		fmt.Printf("\n%-30s Totals: %d/%d objects. Avg: %.2f obj/s. ETA: %v    \n",
			name, context.processedObjects, context.expectedObjects, o_s, eta,
		)
	}
	return nil
}
