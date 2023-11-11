package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	sync "github.com/leapforce-libraries/go_exactonline_new/sync"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type ProjectTimeCostTransaction struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	Timestamp                  int64
	Account                    string
	AccountName                string
	AmountFC                   float64
	Attachment                 string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Currency                   string
	CustomField                string
	Date                       bigquery.NullTimestamp
	Division                   int32
	DivisionDescription        string
	Employee                   string
	EndTime                    bigquery.NullTimestamp
	EntryNumber                int32
	ErrorText                  string
	HourStatus                 int16
	ID                         string
	Item                       string
	ItemDescription            string
	ItemDivisable              bool
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Notes                      string
	PriceFC                    float64
	Project                    string
	ProjectAccount             string
	ProjectAccountCode         string
	ProjectAccountName         string
	ProjectCode                string
	ProjectDescription         string
	Quantity                   float64
	StartTime                  bigquery.NullTimestamp
	Subscription               string
	SubscriptionAccount        string
	SubscriptionAccountCode    string
	SubscriptionAccountName    string
	SubscriptionDescription    string
	SubscriptionNumber         int32
	Type                       int16
	WBS                        string
	WBSDescription             string
}

func getProjectTimeCostTransaction(c *sync.ProjectTimeCostTransaction, softwareClientLicenseGuid string, maxTimestamp *int64) ProjectTimeCostTransaction {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return ProjectTimeCostTransaction{
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.Account.String(),
		c.AccountName,
		c.AmountFC,
		c.Attachment.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.CustomField,
		go_bigquery.DateToNullTimestamp(c.Date),
		c.Division,
		c.DivisionDescription,
		c.Employee.String(),
		go_bigquery.DateToNullTimestamp(c.EndTime),
		c.EntryNumber,
		c.ErrorText,
		c.HourStatus,
		c.ID.String(),
		c.Item.String(),
		c.ItemDescription,
		c.ItemDivisable,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.PriceFC,
		c.Project.String(),
		c.ProjectAccount.String(),
		c.ProjectAccountCode,
		c.ProjectAccountName,
		c.ProjectCode,
		c.ProjectDescription,
		c.Quantity,
		go_bigquery.DateToNullTimestamp(c.StartTime),
		c.Subscription.String(),
		c.SubscriptionAccount.String(),
		c.SubscriptionAccountCode,
		c.SubscriptionAccountName,
		c.SubscriptionDescription,
		c.SubscriptionNumber,
		c.Type,
		c.WBS.String(),
		c.WBSDescription,
	}
}

func (service *Service) WriteProjectTimeCostTransactions(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncProjectTimeCostTransactionsCall(&timestamp)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	maxTimestamp := int64(0)

	for {
		transactionLines, e := call.Do()
		if e != nil {
			return nil, nil, e
		}

		if transactionLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *transactionLines {
			batchRowCount++

			b, err := json.Marshal(getProjectTimeCostTransaction(&tl, softwareClientLicenseGuid, &maxTimestamp))
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#ProjectTimeCostTransactions flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#ProjectTimeCostTransactions: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
