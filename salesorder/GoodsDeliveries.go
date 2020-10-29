package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	salesorder "github.com/Leapforce-nl/go_exactonline_new/salesorder"
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

func (client *Client) GetGoodsDeliveriesBQ(lastModified *time.Time) (*[]GoodsDeliveryBQ, error) {
	gds, err := client.ExactOnline().SalesOrderClient.GetGoodsDeliveries(lastModified)
	if err != nil {
		return nil, err
	}

	if gds == nil {
		return nil, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#GoodsDeliveries for client %s: %v\n", client.ClientID(), rowCount)

	gdsBQ := []GoodsDeliveryBQ{}

	for _, gd := range *gds {
		gdsBQ = append(gdsBQ, getGoodsDeliveryBQ(&gd, client.ClientID()))
	}

	return &gdsBQ, nil
}

func (client *Client) WriteGoodsDeliveriesBQ(writeToObject *storage.ObjectHandle, lastModified *time.Time) (int, interface{}, error) {
	if writeToObject == nil {
		return 0, nil, nil
	}

	gdsBQ, err := client.GetGoodsDeliveriesBQ(lastModified)
	if err != nil {
		return 0, nil, err
	}

	if gdsBQ == nil {
		return 0, nil, nil
	}

	ctx := context.Background()

	w := writeToObject.NewWriter(ctx)

	for _, gdBQ := range *gdsBQ {

		b, err := json.Marshal(gdBQ)
		if err != nil {
			return 0, nil, err
		}

		// Write data
		_, err = w.Write(b)
		if err != nil {
			return 0, nil, err
		}

		// Write NewLine
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			return 0, nil, err
		}
	}

	// Close
	err = w.Close()
	if err != nil {
		return 0, nil, err
	}

	return len(*gdsBQ), GoodsDeliveryBQ{}, nil
}
