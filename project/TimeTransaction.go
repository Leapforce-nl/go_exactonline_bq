package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	project "github.com/leapforce-libraries/go_exactonline_new/project"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type TimeTransaction struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Account                    string
	AccountName                string
	Activity                   string
	ActivityDescription        string
	Amount                     float64
	AmountFC                   float64
	Attachment                 string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Currency                   string
	Date                       bigquery.NullTimestamp
	Division                   int64
	DivisionDescription        string
	Employee                   string
	EndTime                    bigquery.NullTimestamp
	EntryNumber                int64
	ErrorText                  string
	HourStatus                 int64
	Item                       string
	ItemDescription            string
	ItemDivisable              bool
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Notes                      string
	Price                      float64
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
	SubscriptionNumber         int64
	Type                       int64
}

func getTimeTransaction(c *project.TimeTransaction, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) TimeTransaction {
	t := time.Now()

	return TimeTransaction{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		c.Account.String(),
		c.AccountName,
		c.Activity.String(),
		c.ActivityDescription,
		c.Amount,
		c.AmountFC,
		c.Attachment.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		go_bigquery.DateToNullTimestamp(c.Date),
		c.Division,
		c.DivisionDescription,
		c.Employee.String(),
		go_bigquery.DateToNullTimestamp(c.EndTime),
		c.EntryNumber,
		c.ErrorText,
		c.HourStatus,
		c.Item.String(),
		c.ItemDescription,
		c.ItemDivisable,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.Price,
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
	}
}

func (service *Service) WriteTimeTransactions(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.ProjectService().NewGetTimeTransactionsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		timeTransactions, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if timeTransactions == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *timeTransactions {
			batchRowCount++

			b, err := json.Marshal(getTimeTransaction(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
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

			fmt.Printf("#TimeTransactions flushed: %v\n", batchRowCount)

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

	fmt.Printf("#TimeTransactions: %v\n", rowCount)

	return objectHandles, rowCount, TimeTransaction{}, nil
}
