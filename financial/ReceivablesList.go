package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financial "github.com/leapforce-libraries/go_exactonline_new/financial"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type ReceivablesList struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	HID                        string
	AccountCode                string
	AccountId                  string
	AccountName                string
	Amount                     float64
	AmountInTransit            float64
	CurrencyCode               string
	Description                string
	DueDate                    bigquery.NullTimestamp
	EntryNumber                int32
	Id                         string
	InvoiceDate                bigquery.NullTimestamp
	InvoiceNumber              int32
	JournalCode                string
	JournalDescription         string
	YourRef                    string
}

func getReceivablesList(c *financial.ReceivablesList, softwareClientLicenseGuid string) ReceivablesList {
	t := time.Now()

	return ReceivablesList{
		softwareClientLicenseGuid,
		t, t,
		c.HID,
		c.AccountCode,
		c.AccountId.String(),
		c.AccountName,
		c.Amount,
		c.AmountInTransit,
		c.CurrencyCode,
		c.Description,
		go_bigquery.DateToNullTimestamp(c.DueDate),
		c.EntryNumber,
		c.Id.String(),
		go_bigquery.DateToNullTimestamp(c.InvoiceDate),
		c.InvoiceNumber,
		c.JournalCode,
		c.JournalDescription,
		c.YourRef,
	}
}

func (service *Service) WriteReceivablesLists(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialService().NewGetReceivablesListsCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for {
		receivablesLists, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if receivablesLists == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *receivablesLists {
			batchRowCount++

			b, err := json.Marshal(getReceivablesList(&tl, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#ReceivablesLists flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#ReceivablesLists: %v\n", rowCount)

	return objectHandles, rowCount, ReceivablesList{}, nil
}
