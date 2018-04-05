package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"regexp"
	"strings"
	"sync"
)

func walkFiles(done <-chan struct{}, prefix string) (<-chan string, <-chan error) {
	files := make(chan string)
	errc := make(chan error, 1)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		errc <- err
	}

	s3svc := s3.New(sess)

	inputparams := &s3.ListObjectsInput{
		Bucket:  aws.String("amagi-s3logs"),
		MaxKeys: aws.Int64(20),
		Prefix:  aws.String(prefix),
	}

	go func() {
		defer close(files)

		pageNum := 0
		errc <- s3svc.ListObjectsPages(inputparams, func(page *s3.ListObjectsOutput, lastPage bool) bool {
			fmt.Println("Page", pageNum)
			pageNum++
			for _, value := range page.Contents {
				//fmt.Println(*value.Key)

				select {
				case files <- *value.Key:
				case <-done:
					fmt.Println("aborting")
					return false
				}
			}
			//fmt.Println("pageNum", pageNum, "lastPage", lastPage)

			return true
		})
	}()

	return files, errc
}

func search(done <-chan struct{}, regex *regexp.Regexp, files <-chan string, c chan<- string) {
	for file := range files {
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("us-east-1"),
		})

		svc := s3.New(sess)

		input := &s3.GetObjectInput{
			Bucket: aws.String("amagi-s3logs"),
			Key:    aws.String(file),
		}

		fmt.Println("Downloading ", *input.Key)
		result, err := svc.GetObject(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchKey:
					fmt.Println(s3.ErrCodeNoSuchKey, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				fmt.Println(err.Error())
			}
		}

		buf := new(bytes.Buffer)
		buf.ReadFrom(result.Body)
		data := buf.String()
		//fmt.Println(data)
		lines := strings.Split(data, "\n")
		for i, line := range lines {
			res := regex.MatchString(line)
			log := fmt.Sprintf("%s:%d %s\n", *input.Key, i+1, line)
			if res {
				select {
				case c <- log:
				case <-done:
					return
				}
			}
		}
	}
}

func pgrep(prefix string, regex string, outputFile string) error {
	done := make(chan struct{})
	defer close(done)

	files, errc := walkFiles(done, prefix)

	rgx, err := regexp.Compile(regex)
	if err != nil {
		return err
	}

	c := make(chan string)
	var wg sync.WaitGroup
	const numSearchs = 20
	wg.Add(numSearchs)

	for i := 0; i < numSearchs; i++ {
		go func() {
			search(done, rgx, files, c)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(c)
	}()

	output, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer output.Close()

	for r := range c {
		if _, err = output.WriteString(r); err != nil {
			return err
		}
	}

	if err := <-errc; err != nil {
		return err
	}
	return nil
}

//go run main.go "b4u" "BC5090529F49793F" output
//go run main.go "combatgo/2018-03-24" "DE48B79718DA34F6" output1
func main() {
	err := pgrep(os.Args[1], os.Args[2], os.Args[3])
	if err != nil {
		fmt.Println("Failed with error ", err)
	}
}
