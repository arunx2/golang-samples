package main

import (
	"fmt"
	"github.com/namsral/flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var count int32

// GetQueueURL gets the URL of an Amazon SQS queue
// Inputs:
//     sess is the current session, which provides configuration for the SDK's service clients
//     queueName is the name of the queue
// Output:
//     If success, the URL of the queue and nil
//     Otherwise, an empty string and an error from the call to
func GetQueueURL(sess *session.Session, queue *string) (*sqs.GetQueueUrlOutput, error) {
	// Create an SQS service client
	svc := sqs.New(sess)

	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: queue,
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// SendMsg sends a message to an Amazon SQS queue
// Inputs:
//     sess is the current session, which provides configuration for the SDK's service clients
//     queueURL is the URL of the queue
// Output:
//     If success, nil
//     Otherwise, an error from the call to SendMessage
func SendMsg(sess *session.Session, data string, id string, msgType string, queueURL *string) error {
	// Create an SQS service client
	svc := sqs.New(sess)
	var err error

	_, err = svc.SendMessage(&sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"msgType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(msgType),
			},
			"id": {
				DataType:    aws.String("String"),
				StringValue: aws.String(id),
			},
		},
		MessageBody: aws.String(data),
		QueueUrl:    queueURL,
	})

	if err != nil {
		return err
	}

	return nil
}

func main() {

	queue := flag.String("q", "", "The name of the queue")
	flag.Parse()

	if *queue == "" {
		fmt.Println("You must supply the name of a queue (-q QUEUE)")
		return
	}


	// Create a session that gets credential values from ~/.aws/credentials
	// and the default region from ~/.aws/config

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))


	// Get URL of queue
	result, err := GetQueueURL(sess, queue)
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return
	}

	queueURL := result.QueueUrl
	// load all files from the directory
	filepath.Walk("./resources/imdbdata",
		func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				count++
				fmt.Printf("\n loading... Path = %s, Size = %d, Count = %d", path, info.Size(), count)
				data, _ := ioutil.ReadFile(path)
				err = SendMsg(sess, string(data), strings.TrimSuffix(info.Name(),".json"), "msgType", queueURL)
			}
			return nil
		})

	fmt.Println("Sent message to queue ")
}
