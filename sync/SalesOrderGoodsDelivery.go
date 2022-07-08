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

type SalesOrderGoodsDelivery struct {
	SoftwareClientLicenseGuid_    string
	Created_                      time.Time
	Modified_                     time.Time
	Timestamp                     int64
	EntryID                       string
	Created                       bigquery.NullTimestamp
	Creator                       string
	CreatorFullName               string
	DeliveryAccount               string
	DeliveryAccountCode           string
	DeliveryAccountName           string
	DeliveryAddress               string
	DeliveryContact               string
	DeliveryContactPersonFullName string
	DeliveryDate                  bigquery.NullTimestamp
	DeliveryNumber                int32
	Description                   string
	Division                      int32
	Document                      string
	DocumentSubject               string
	EntryNumber                   int32
	Modified                      bigquery.NullTimestamp
	Modifier                      string
	ModifierFullName              string
	Remarks                       string
	ShippingMethod                string
	ShippingMethodCode            string
	ShippingMethodDescription     string
	TrackingNumber                string
	Warehouse                     string
	WarehouseCode                 string
	WarehouseDescription          string
}

func getSalesOrderGoodsDelivery(c *sync.SalesOrderGoodsDelivery, softwareClientLicenseGuid string, maxTimestamp *int64) SalesOrderGoodsDelivery {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return SalesOrderGoodsDelivery{
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.EntryID.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.DeliveryAccount.String(),
		c.DeliveryAccountCode,
		c.DeliveryAccountName,
		c.DeliveryAddress.String(),
		c.DeliveryContact.String(),
		c.DeliveryContactPersonFullName,
		go_bigquery.DateToNullTimestamp(c.DeliveryDate),
		c.DeliveryNumber,
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentSubject,
		c.EntryNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Remarks,
		c.ShippingMethod.String(),
		c.ShippingMethodCode,
		c.ShippingMethodDescription,
		c.TrackingNumber,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
	}
}

func (service *Service) WriteSalesOrderGoodsDeliveries(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncSalesOrderGoodsDeliveriesCall(&timestamp)

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

			b, err := json.Marshal(getSalesOrderGoodsDelivery(&tl, softwareClientLicenseGuid, &maxTimestamp))
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

			fmt.Printf("#SalesOrderGoodsDeliveries flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesOrderGoodsDeliveries: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
