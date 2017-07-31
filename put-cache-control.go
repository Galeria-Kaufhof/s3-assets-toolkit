/* put-cache-control copies objects in S3 bucket in-place, modifying the meta data, e.g. Cache-Control headers */
package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jessevdk/go-flags"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

/*
flags:



*/

type Opts struct {
	TargetBucket string `short: "t", long: "target-bucket"`

	InPlace    bool   `long: "in-place"`
	FromBucket string `long: "from-bucket"`
	// TODO validation - Please provide the source of the objects. Either --in-place or --from-bucket must be specified. Or use InPlace as the default for FromBucket
	SetCacheControl `long: "set-cache-control"`
	// cache for one year - so almost forever
	SetCacheControlForever `long: "set-cache-control-forever"`
}

var opts Opts

// modify aws s3 copy args in place
func args_src_copy_from(key string, opts Opts, input *s3.CopyObjectInput) {
	input.SetCopySource(aws.String(fromBucket + key))
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
	prepareCopy(object, Opts)
	svc := s3.New(session.New())
}

func prepareCopy(key string, opts Opts) {
	object = load_existing_metadata(key)
	input := &s3.CopyObjectInput{
		Bucket: aws.String(opts.TargetBucket),
		Key:    aws.String(key),
	}
	args_src_copy_from(key, opts, input)
	args_src_cache_control(object, opts, input)
	args_src_existing_meta(object, opts, input)
}

func main() {
	context, _ := prepareContext()
	parallelity := 200 // set well below the typical ulimit of 1024
	// to avoid "socket: too many open files".
	// Also fits AWS API limits, avoid "503 SlowDown: Please reduce your request rate."

	names := make(chan string)

	context.wg.Add(parallelity)
	for gr := 1; gr <= parallelity; gr++ {
		go cpworker(&context, names)
	}

	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		names <- input.Text()
	}
	close(names)

	context.wg.Wait()
	fmt.Printf("\nDone.\n")
}

/* Hello world */
func CheckPublicCommentTmp() {
}

/* CopyContext defines context for running concurrent copy operations and remembers the progress */
type CopyContext struct {
	s3svc      *s3.S3
	bucketname string
	newvalue   string

	copiedObjects   int64
	copiedBytes     int64
	expectedObjects int64
	start           time.Time
	statusLineMutex sync.Mutex
	lastStatusShown float64

	wg sync.WaitGroup
}

func prepareContext() (CopyContext, error) {
	// Session with the new library
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1")},
	)
	if err != nil {
		panic(fmt.Sprintf("Can not create AWS SDK session %s", err))
	}

	if len(os.Args) != 3 {
		panic("Please provide bucket name and desired Cache-Control setting")
	}
	return CopyContext{
		s3svc:           s3.New(sess),
		bucketname:      os.Args[1],
		expectedObjects: 3867874,
		newvalue:        os.Args[2],
		start:           time.Now(),
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

func cp(context *CopyContext, name string) error {
	//fmt.Println(context.bucketname)
	//fmt.Println(url.PathEscape(name))
	headin := s3.HeadObjectInput{
		Bucket: aws.String(context.bucketname),
		Key:    aws.String(name),
		//		Key:    aws.String(url.PathEscape(name)),
	}
	headresp, err := context.s3svc.HeadObject(&headin)
	if err != nil {
		return fmt.Errorf("aws sdk Head failed: %v", err)
	}

	contenttype := headresp.ContentType
	oldcachecontrol := headresp.CacheControl

	var status string
	// . - skip
	// X - type was not set, set to image/png
	// j - was image/jpeg; adjusted CacheControl
	// g - was image/png; adjusted CacheControl
	// P - pdf file; adjusted CacheControl
	// Y - other file type; adjusted CacheControl
	if oldcachecontrol != nil && *oldcachecontrol == context.newvalue {
		status = "."
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

		src := fmt.Sprintf("%s/%s", context.bucketname, url.PathEscape(name))
		inp := s3.CopyObjectInput{
			Bucket:            aws.String(context.bucketname),
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
	}
	fmt.Print(status)

	atomic.AddInt64(&context.copiedObjects, 1)
	sec := time.Since(context.start).Seconds()
	o_s := float64(context.copiedObjects) / sec
	expectedDuration := time.Duration(int(float64(context.expectedObjects-context.copiedObjects)/o_s)) * time.Second
	days := int(expectedDuration.Hours() / 24)
	andHours := expectedDuration.Hours() - float64(days)*24
	eta := fmt.Sprintf("%dd %.1fh", days, andHours)
	if days == 0 {
		hours := int(expectedDuration.Minutes() / 60)
		andMinutes := expectedDuration.Minutes() - float64(hours)*60
		eta = fmt.Sprintf("%dh %.1fm", hours, andMinutes)
	}
	if context.expectedObjects < context.copiedObjects {
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
			name, context.copiedObjects, context.expectedObjects, o_s, eta,
		)
	}
	return nil
}
