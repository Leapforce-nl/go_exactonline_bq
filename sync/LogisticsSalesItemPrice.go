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

type LogisticsSalesItemPrice struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	Created_                   time.Time
	Modified_                  time.Time
	Timestamp                  int64
	ID                         string
	Account                    string
	AccountName                string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Currency                   string
	DefaultItemUnit            string
	DefaultItemUnitDescription string
	Division                   int32
	EndDate                    bigquery.NullTimestamp
	Item                       string
	ItemCode                   string
	ItemDescription            string
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	NumberOfItemsPerUnit       float64
	Price                      float64
	Quantity                   float64
	StartDate                  bigquery.NullTimestamp
	Unit                       string
	UnitDescription            string
}

func getLogisticsSalesItemPrice(c *sync.LogisticsSalesItemPrice, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) LogisticsSalesItemPrice {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return LogisticsSalesItemPrice{
		organisationID,
		softwareClientLicenceID,
		t, t,
		timestamp,
		c.ID.String(),
		c.Account.String(),
		c.AccountName,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.DefaultItemUnit,
		c.DefaultItemUnitDescription,
		c.Division,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NumberOfItemsPerUnit,
		c.Price,
		c.Quantity,
		go_bigquery.DateToNullTimestamp(c.StartDate),
		c.Unit,
		c.UnitDescription,
	}
}

func (service *Service) WriteLogisticsSalesItemPrices(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncLogisticsSalesItemPricesCall(&timestamp)

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

			b, err := json.Marshal(getLogisticsSalesItemPrice(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
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

			fmt.Printf("#LogisticsSalesItemPrices flushed: %v\n", batchRowCount)

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

	fmt.Printf("#LogisticsSalesItemPrices: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
