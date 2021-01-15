package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	_bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	salesorder "github.com/leapforce-libraries/go_exactonline_new/salesorder"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type GoodsDeliveryLineBQ struct {
	ClientID string
	ID       string
	//BatchNumbers
	Created              _bigquery.NullTimestamp
	Creator              string
	CreatorFullName      string
	CustomerItemCode     string
	DeliveryDate         _bigquery.NullTimestamp
	Description          string
	Division             int32
	EntryID              string
	Item                 string
	ItemCode             string
	ItemDescription      string
	LineNumber           int32
	Modified             _bigquery.NullTimestamp
	Modifier             string
	ModifierFullName     string
	Notes                string
	QuantityDelivered    float64
	QuantityOrdered      float64
	SalesOrderLineID     string
	SalesOrderLineNumber int32
	SalesOrderNumber     int32
	//SerialNumbers
	StorageLocation            string
	StorageLocationCode        string
	StorageLocationDescription string
	TrackingNumber             string
	Unitcode                   string
}

func getGoodsDeliveryLineBQ(c *salesorder.GoodsDeliveryLine, clientID string) GoodsDeliveryLineBQ {
	return GoodsDeliveryLineBQ{
		clientID,
		c.ID.String(),
		//c.BatchNumbers,
		bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.CustomerItemCode,
		bigquery.DateToNullTimestamp(c.DeliveryDate),
		c.Description,
		c.Division,
		c.EntryID.String(),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.LineNumber,
		bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.QuantityDelivered,
		c.QuantityOrdered,
		c.SalesOrderLineID.String(),
		c.SalesOrderLineNumber,
		c.SalesOrderNumber,
		//c.SerialNumbers,
		c.StorageLocation.String(),
		c.StorageLocationCode,
		c.StorageLocationDescription,
		c.TrackingNumber,
		c.Unitcode,
	}
}

func (service *Service) WriteGoodsDeliveryLinesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SalesOrderService().NewGetGoodsDeliveryLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		goodsDeliveryLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if goodsDeliveryLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *goodsDeliveryLines {
			batchRowCount++

			b, err := json.Marshal(getGoodsDeliveryLineBQ(&tl, service.ClientID()))
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

			fmt.Printf("#GoodsDeliveryLines for service %s flushed: %v\n", service.ClientID(), batchRowCount)

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

	fmt.Printf("#GoodsDeliveryLines for service %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, GoodsDeliveryLineBQ{}, nil
}
