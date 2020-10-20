package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	exactonline "github.com/Leapforce-nl/exactonline_bq2/exactonline"
	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	salesorder "github.com/Leapforce-nl/go_exactonline2/salesorder"
)

type GoodsDeliveries struct {
}

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

func (_ *GoodsDeliveries) Table() *exactonline.Table {
	return exactonline.NewTable(
		"salesorder_goodsdeliveries",
		"salesorder_goodsdeliveries",
		GoodsDeliveryBQ{})
}

func (_ *GoodsDeliveries) GetDataAndWriteToBucket(bq *bigquerytools.BigQuery, obj *storage.ObjectHandle, client *exactonline.Client) (int, error) {
	if client == nil {
		return 0, nil
	}

	ctx := context.Background()

	w := obj.NewWriter(ctx)

	gds, err := client.ExactOnline.SalesOrderClient.GetGoodsDeliveries()
	if err != nil {
		return 0, err
	}

	if gds == nil {
		return 0, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#GoodsDeliveries for client %s: %v\n", client.ClientID, rowCount)

	for _, a := range *gds {
		b, err := json.Marshal(getGoodsDeliveryBQ(&a, client.ClientID))
		if err != nil {
			return 0, err
		}

		// Write data
		_, err = w.Write(b)
		if err != nil {
			return 0, err
		}

		// Write NewLine
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			return 0, err
		}
	}

	// Close
	err = w.Close()
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}
