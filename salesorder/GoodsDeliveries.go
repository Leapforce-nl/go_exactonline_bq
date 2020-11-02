package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/leapforce-libraries/go_bigquerytools"
	salesorder "github.com/leapforce-libraries/go_exactonline_new/salesorder"
	types "github.com/leapforce-libraries/go_types"
)

type GoodsDeliveryBQ struct {
	ClientID                      string
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
	//GoodsDeliveryLines
	Modified                  bigquery.NullTimestamp
	Modifier                  string
	ModifierFullName          string
	Remarks                   string
	ShippingMethod            string
	ShippingMethodCode        string
	ShippingMethodDescription string
	TrackingNumber            string
	Warehouse                 string
	WarehouseCode             string
	WarehouseDescription      string
}

func getGoodsDeliveryBQ(c *salesorder.GoodsDelivery, clientID string) GoodsDeliveryBQ {
	return GoodsDeliveryBQ{
		clientID,
		c.EntryID.String(),
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.DeliveryAccount.String(),
		c.DeliveryAccountCode,
		c.DeliveryAccountName,
		c.DeliveryAddress.String(),
		c.DeliveryContact.String(),
		c.DeliveryContactPersonFullName,
		bigquerytools.DateToNullTimestamp(c.DeliveryDate),
		c.DeliveryNumber,
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentSubject,
		c.EntryNumber,
		bigquerytools.DateToNullTimestamp(c.Modified),
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

func (client *Client) WriteGoodsDeliveriesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := client.ExactOnline().SalesOrderClient.NewGetGoodsDeliveriesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		goodsDeliveries, err := call.Do()
		if err != nil {
			return nil, 0, nil, err
		}

		if goodsDeliveries == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *goodsDeliveries {
			batchRowCount++

			b, err := json.Marshal(getGoodsDeliveryBQ(&tl, client.ClientID()))
			if err != nil {
				return nil, 0, nil, err
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, err
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, err
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err = w.Close()
			if err != nil {
				return nil, 0, nil, err
			}
			w = nil

			fmt.Printf("#GoodsDeliveries for client %s flushed: %v\n", client.ClientID(), batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, err
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#GoodsDeliveries for client %s: %v\n", client.ClientID(), rowCount)

	return objectHandles, rowCount, GoodsDeliveryBQ{}, nil
}
