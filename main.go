package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	queue := "https://sqs.ap-southeast-2.amazonaws.com/675679404987/golang_test"

	process := make(chan *sqs.Message, 1000)
	delete := make(chan *sqs.Message, 1000)
	done := make(chan bool)
	go getMessages(queue, svc, process)
	go processMessages(process, delete)
	go deleteMessages(queue, svc, delete, done)

	<-done

	fmt.Println("Quitting")
}

func processMessages(process chan *sqs.Message, delete chan *sqs.Message) {
	for {
		msg, open := <-process

		if open {
			fmt.Printf("Message %v processed\n", msg.MessageId)
			delete <- msg
		} else {
			close(delete)
			break
		}
	}
}

func deleteMessages(queue string, client *sqs.SQS, delete <-chan *sqs.Message, done chan bool) {
	for {
		request := &sqs.DeleteMessageBatchInput{
			QueueUrl: &queue,
		}

		entries := make([]*sqs.DeleteMessageBatchRequestEntry, 0)

		completed := false

		for i := 0; i < 10; i++ {
			select {
			case m, more := <-delete:

				if more {
					entries = append(entries, &sqs.DeleteMessageBatchRequestEntry{
						Id:            m.MessageId,
						ReceiptHandle: m.ReceiptHandle,
					})
				} else {
					completed = true
					break
				}
			default:
				break
			}
		}

		if len(entries) > 0 {
			request.SetEntries(entries)
			_, err := client.DeleteMessageBatch(request)

			if err != nil {
				fmt.Println("Error", err)
			}
		} else if completed {
			done <- true
		}
	}
}

func getMessages(queue string, client *sqs.SQS, process chan *sqs.Message) {

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            &queue,
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(0),
	}

	for {
		response, err := client.ReceiveMessage(params)
		if err != nil {
			fmt.Println("Failed to receive messages", err)
			return
		}

		for _, msg := range response.Messages {
			process <- msg
		}

		if len(response.Messages) == 0 {
			close(process)
			break
		}
	}
}
