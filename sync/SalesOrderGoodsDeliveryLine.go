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

type SalesOrderGoodsDeliveryLine struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Created_                 time.Time
	Modified_                time.Time
	Timestamp                int64
	ID                       string
	//BatchNumbers
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	CustomerItemCode           string
	DeliveryDate               bigquery.NullTimestamp
	Description                string
	Division                   int32
	EntryID                    string
	Item                       string
	ItemCode                   string
	ItemDescription            string
	LineNumber                 int32
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Notes                      string
	QuantityDelivered          float64
	QuantityOrdered            float64
	SalesOrderLineID           string
	SalesOrderLineNumber       int32
	SalesOrderNumber           int32
	StorageLocation            string
	StorageLocationCode        string
	StorageLocationDescription string
	TrackingNumber             string
	Unitcode                   string
}

func getSalesOrderGoodsDeliveryLine(c *sync.SalesOrderGoodsDeliveryLine, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) SalesOrderGoodsDeliveryLine {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return SalesOrderGoodsDeliveryLine{
		organisationID,
		softwareClientLicenceID,
		t, t,
		timestamp,
		c.ID.String(),
		//c.BatchNumbers,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.CustomerItemCode,
		go_bigquery.DateToNullTimestamp(c.DeliveryDate),
		c.Description,
		c.Division,
		c.EntryID.String(),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.LineNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.QuantityDelivered,
		c.QuantityOrdered,
		c.SalesOrderLineID.String(),
		c.SalesOrderLineNumber,
		c.SalesOrderNumber,
		c.StorageLocation.String(),
		c.StorageLocationCode,
		c.StorageLocationDescription,
		c.TrackingNumber,
		c.Unitcode,
	}
}

func (service *Service) WriteSalesOrderGoodsDeliveryLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncSalesOrderGoodsDeliveryLinesCall(&timestamp)

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

			b, err := json.Marshal(getSalesOrderGoodsDeliveryLine(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
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

			fmt.Printf("#SalesOrderGoodsDeliveryLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesOrderGoodsDeliveryLines: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
