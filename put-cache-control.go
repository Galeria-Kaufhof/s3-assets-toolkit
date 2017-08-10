package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"gopkg.in/urfave/cli.v1"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func assumeRoleCrossAccount(role string) (*aws.Config, error) {
	security := sts.New(session.New())
	input := &sts.AssumeRoleInput{
		DurationSeconds: aws.Int64(3600),
		ExternalId:      aws.String("123ABC"),
		RoleArn:         &role,
		RoleSessionName: aws.String("PutCacheControlImpersonification"),
	}
	impersonated, err := security.AssumeRole(input)
	if err != nil {
		return nil, fmt.Errorf("assume role '%s' for cross-account access failed: %v", role, err)
	}

	c := *impersonated.Credentials
	tmpCreds := credentials.NewStaticCredentials(*c.AccessKeyId, *c.SecretAccessKey, *c.SessionToken)
	return aws.NewConfig().WithCredentials(tmpCreds), nil
}

// Find out the number of objects in the bucket
// func getBucketSize(svc cloudwatch.CloudWatch) (int64, error) {
func getBucketSize(bucketName string, conf *aws.Config) (int64, error) {
	svcCrossAccount := cloudwatch.New(session.New(), conf)
	dims := []*cloudwatch.Dimension{
		&cloudwatch.Dimension{Name: aws.String("BucketName"), Value: aws.String(bucketName)},
		&cloudwatch.Dimension{Name: aws.String("StorageType"), Value: aws.String("AllStorageTypes")},
	}
	req := cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/S3"),
		StartTime:  aws.Time(time.Now().Add(-time.Hour * 24 * 3)),
		EndTime:    aws.Time(time.Now()),
		Period:     aws.Int64(3600), // TODO try out Period: 86400 (one day)
		Statistics: []*string{aws.String("Maximum")},
		MetricName: aws.String("NumberOfObjects"),
		Dimensions: dims,
	}
	resp, err := svcCrossAccount.GetMetricStatistics(&req)
	if err != nil {
		return 0, fmt.Errorf("Failed to detect bucket '%s' size: %v", bucketName, err)
	}

	if len(resp.Datapoints) == 0 {
		return 0, fmt.Errorf("No object counting for source bucket possible. Remember to give reading user `cloudwatch:GetMetricData` permission on source bucket. And provide --cross-account-cloudwatch-role")
	}
	max := 0.0
	for _, dp := range resp.Datapoints {
		if *dp.Maximum > max {
			max = *dp.Maximum
		}
	}
	return int64(max), nil
}

// Quickly find out the size of the bucket to copy for a nice progress indicator.
// Side effect: modifies the context
func getExpectedSize(context *CopyContext) {
	var err error
	context.expectedObjects, err = getBucketSize(context.from, &aws.Config{})
	if err != nil && context.cloudwatchRole != "" {
		// Retry getBucketSize using assume role (cross-account),
		// first acquire temporary cross-account credentials (AWS STS)
		confCrossAccount, errRole := assumeRoleCrossAccount(context.cloudwatchRole)
		if errRole != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed to detect 'from' bucket size: %v\n", errRole))
			context.expectedObjects = 0 // unknown
			return
		}
		context.expectedObjects, err = getBucketSize(context.from, confCrossAccount)
	}
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to detect 'from' bucket size: %v\n", err))
		context.expectedObjects = 0 // unknown
		return
	}
	fmt.Printf("Objects to copy/check: %d\n", context.expectedObjects)
}

func listObjectsFromStdin(names chan<- string) {
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		names <- input.Text()
	}
}

func listObjectsToCopy(names chan<- string, bucketname string, continueFromKey string, context *CopyContext) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketname),
		MaxKeys: aws.Int64(1000),
	}
	if continueFromKey != "" {
		input.StartAfter = &continueFromKey
	}

	err := context.s3svc.ListObjectsV2Pages(input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			// Could use following if cloudwatch based metrics are not available:
			// atomic.AddInt64(&context.expectedObjects, int64(len(page.Contents)))
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
		cli.IntFlag{
			Name:  "parallelity, p",
			Value: 200,
			Usage: "number of workers to use",
		},
		cli.BoolFlag{
			Name:  "noop",
			Usage: "make no changes, just gather statistics",
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
		cli.StringFlag{
			Name:  "continue, u",
			Usage: "do not start over, continue from given key",
		},
		cli.BoolFlag{
			Name:  "stdin",
			Usage: "take file names to copy from stdin",
		},
		cli.StringFlag{
			Name: "cross-account-cloudwatch-role, r",
			Usage: `

	Sometimes you need to copy objects between buckets from different accounts
	(cross-account), e.g. prod- vs. nonprod- account. Obviously you need to give
	the account you currently use write permission to the target bucket and read
	permission to the 'from' bucket. But to also have a correct progress bar for
	the long running copy operation, you need to give your account the permission
	to access cloudwatch metrics for the 'from' bucket.

			`,
		},
	}
	app.Action = func(c *cli.Context) error {
		context, _ := prepareContextFromCli(c)

		// set well below the typical ulimit of 1024 - TODO add to docs
		// to avoid "socket: too many open files".
		// Also fits AWS API limits, avoid "503 SlowDown: Please reduce your request rate."
		parallelity := c.GlobalInt("parallelity")

		names := make(chan string, 3000)
		context.wg.Add(parallelity)
		for gr := 1; gr <= parallelity; gr++ {
			go cpworker(&context, names)
		}

		getExpectedSize(&context)
		if c.GlobalBool("stdin") {
			listObjectsFromStdin(names)
		} else {
			listObjectsToCopy(names, context.from, c.GlobalString("continue"), &context)
		}
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
	s3svc          *s3.S3
	target         string
	from           string
	newvalue       string
	exclude        regexp.Regexp
	cloudwatchRole string
	noop           bool

	copiedObjects    int64
	maxObjectsToCopy int64
	processedObjects int64 // including ignored and skipped
	copiedBytes      int64
	expectedObjects  int64
	start            time.Time
	statusLineMutex  sync.Mutex
	lastStatusShown  float64
	statsMutex       sync.Mutex
	statusStats      map[string]int
	typeStats        map[string]int

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
		noop:             c.GlobalBool("noop"),
		expectedObjects:  0,
		maxObjectsToCopy: c.GlobalInt64("first-n"),
		newvalue:         c.GlobalString("cache-control"),
		exclude:          *regexp.MustCompile(exclude_pattern),
		cloudwatchRole:   c.GlobalString("cross-account-cloudwatch-role"),
		start:            time.Now(),
		statusStats:      make(map[string]int),
		typeStats:        make(map[string]int),
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

func str(o *s3.HeadObjectOutput) string {
	if o == nil {
		return "NotFound"
	} else {
		cache, ctype := "nil", "nil"
		if o.CacheControl != nil {
			cache = *o.CacheControl
		}
		if o.ContentType != nil {
			ctype = *o.ContentType
		}
		return fmt.Sprintf("%s, %s", cache, ctype)
	}
}

func cp(context *CopyContext, name string) error {
	//fmt.Println(context.target)
	//fmt.Println(url.PathEscape(name))
	// key := aws.String(url.PathEscape(name)),
	key := name
	from, fromErr := context.s3svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(context.from),
		Key:    aws.String(key),
	})
	if fromErr != nil {
		return fmt.Errorf("\naws sdk Head for `%s` failed: \n%T\n%v\n", key, fromErr, fromErr)
	}

	contenttype := from.ContentType

	target, targetErr := context.s3svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(context.target),
		Key:    aws.String(key),
	})
	if targetErr != nil {
		if aerr, ok := targetErr.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound":
				target = nil
			default:
				os.Stderr.WriteString(fmt.Sprintf("\n***Missing target Head for `%s` failed (code %s): \n%T\n%v\n",
					key, aerr.Code(), targetErr, targetErr))
			}
		} else {
			return fmt.Errorf("\naws sdk Head for target `%s` failed, can not recognize the aws return code: \n%T\n%v\n",
				key, fromErr, fromErr)
		}
	}

	var status string
	// E - excluded pattern
	// . - skip, Cache-Control and Content-Type already set
	// X - type was not set, set to image/png
	// j - was image/jpeg; adjusted CacheControl
	// g - was image/png; adjusted CacheControl
	// P - pdf file; adjusted CacheControl
	// Y - other file type; adjusted CacheControl
	if context.exclude.MatchString(name) && IsPicture(from) {
		status = "E"
	} else if target != nil && target.CacheControl != nil && *target.CacheControl == context.newvalue &&
		target.ContentType != nil {
		status = "."
	} else if context.copiedObjects > context.maxObjectsToCopy {
		status = ","
	} else {
		if contenttype == nil {
			status = "X"
			contenttype = aws.String("image/png")
		} else {
			// DEBUG: fmt.Printf("\nkey %s, from: %s target: %s\n", key, str(from), str(target))
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
		if !context.noop {
			_, err := context.s3svc.CopyObject(&inp)
			if err != nil {
				return fmt.Errorf("Failed changing (inplace-copying) object: %v", err)
			}
		}
		atomic.AddInt64(&context.copiedObjects, 1)
	}
	fmt.Print(status)
	context.statsMutex.Lock()
	context.statusStats[status] += 1
	// extract interesting part before semicolon, like "mulitpart/package"
	// from `multipart/package; boundary="_-------------1437962543790"`
	ctype := strings.Split(*contenttype, ";")[0]
	context.typeStats[ctype] += 1

	context.statsMutex.Unlock()

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
		context.statsMutex.Lock()
		fmt.Printf("\n%-30s Totals: %d/%d objects. Avg: %.2f obj/s. ETA: %v    \n",
			name, context.processedObjects, context.expectedObjects, o_s, eta,
		)
		fmt.Printf("\nContent-Type stats:\n")
		for k, v := range context.typeStats {
			fmt.Printf("%s %d\n", k, v)
		}
		fmt.Printf("\nCopy status stats:\n")
		for k, v := range context.statusStats {
			fmt.Printf("%s %d\n", k, v)
		}
		context.statsMutex.Unlock()
	}
	return nil
}
