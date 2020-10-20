package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	salesorder "github.com/Leapforce-nl/go_exactonline_new/salesorder"
)

type GoodsDeliveryLineBQ struct {
	ClientID string
	ID       string
	//BatchNumbers
	Created              bigquery.NullTimestamp
	Creator              string
	CreatorFullName      string
	CustomerItemCode     string
	DeliveryDate         bigquery.NullTimestamp
	Description          string
	Division             int32
	EntryID              string
	Item                 string
	ItemCode             string
	ItemDescription      string
	LineNumber           int32
	Modified             bigquery.NullTimestamp
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
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.CustomerItemCode,
		bigquerytools.DateToNullTimestamp(c.DeliveryDate),
		c.Description,
		c.Division,
		c.EntryID.String(),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.LineNumber,
		bigquerytools.DateToNullTimestamp(c.Modified),
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

func (client *Client) GetGoodsDeliveryLinesBQ() (*[]GoodsDeliveryLineBQ, error) {
	gds, err := client.ExactOnline().SalesOrderClient.GetGoodsDeliveryLines()
	if err != nil {
		return nil, err
	}

	if gds == nil {
		return nil, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#GoodsDeliveryLines for client %s: %v\n", client.ClientID(), rowCount)

	gdsBQ := []GoodsDeliveryLineBQ{}

	for _, gd := range *gds {
		gdsBQ = append(gdsBQ, getGoodsDeliveryLineBQ(&gd, client.ClientID()))
	}

	return &gdsBQ, nil
}

func (client *Client) WriteGoodsDeliveryLinesBQ(writeToObject *storage.ObjectHandle) (interface{}, error) {
	if writeToObject == nil {
		return nil, nil
	}

	gdsBQ, err := client.GetGoodsDeliveryLinesBQ()
	if err != nil {
		return nil, err
	}

	if gdsBQ == nil {
		return nil, nil
	}

	ctx := context.Background()

	w := writeToObject.NewWriter(ctx)

	for _, gdBQ := range *gdsBQ {

		b, err := json.Marshal(gdBQ)
		if err != nil {
			return nil, err
		}

		// Write data
		_, err = w.Write(b)
		if err != nil {
			return nil, err
		}

		// Write NewLine
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			return nil, err
		}
	}

	// Close
	err = w.Close()
	if err != nil {
		return nil, err
	}

	return GoodsDeliveryLineBQ{}, nil
}
