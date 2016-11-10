package main

import (
	"bufio"
	"fmt"
	"github.com/AdRoll/goamz/aws" //traditional library by Canonical + AdRoll
	"github.com/AdRoll/goamz/s3"
	sdkaws "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session" // the new library by aws
	sdks3 "github.com/aws/aws-sdk-go/service/s3"
	"net/http"
	"os"
	//	"os/exec"
	//	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	context, _ := prepareContext()
	parallelity := 10

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

type CopyContext struct {
	s3         *s3.Bucket
	s3svc      *sdks3.S3
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
	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err.Error())
	}
	// Using old library, TODO: switch everything to the new
	s := s3.New(auth, aws.EUCentral)
	s.Signature = aws.V4Signature

	// Session with the new library
	sess, err := session.NewSession(&sdkaws.Config{
		Region: sdkaws.String("eu-central-1")},
	)
	if err != nil {
		panic(fmt.Sprintf("Can not create AWS SDK session %s", err))
	}

	if len(os.Args) != 3 {
		panic("Please provide bucket name and desired Cache-Control setting")
	}
	return CopyContext{
		s3:              s.Bucket(os.Args[1]),
		s3svc:           sdks3.New(sess),
		bucketname:      os.Args[1],
		expectedObjects: 18000000,
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
			fmt.Printf("\nNo more data in names channel.\n")
			context.wg.Done()
			return
		}
	}
}

func cp(context *CopyContext, name string) error {
	resp, err := context.s3.Head(name, make(http.Header))
	if err != nil {
		return fmt.Errorf("HEAD failed for '%s' Error: %v", name, err)
	}

	contenttypes, contenttype_present := resp.Header["Content-Type"]
	cache, cache_present := resp.Header["Cache-Control"]

	var status, contenttype string
	// . - skip
	// X - type was not set, set to image/png
	// j - was image/jpeg; adjusted CacheControl
	// g - was image/png; adjusted CacheControl
	// P - pdf file; adjusted CacheControl
	// Y - other file type; adjusted CacheControl
	if cache_present && len(cache) == 1 && cache[0] == context.newvalue {
		status = "."
	} else {
		if !contenttype_present {
			status = "X"
			contenttype = "image/png"
		} else {
			contenttype = contenttypes[0] // theoretically, there can be multiple HTTP headers with the same key
			// but lets assume, there is at most one
			if contenttype == "image/png" {
				status = "g"
			} else if contenttype == "image/jpeg" {
				status = "j"
			} else if contenttype == "application/pdf" {
				status = "P"
			} else {
				status = "Y"
			}
		}

		src := fmt.Sprintf("%s/%s", context.bucketname, name)
		inp := sdks3.CopyObjectInput{
			Bucket:            sdkaws.String(context.bucketname),
			CopySource:        &src,
			Key:               &name,
			CacheControl:      &context.newvalue,
			ContentType:       &contenttype,
			MetadataDirective: sdkaws.String("REPLACE"),
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
