package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/leapforce-libraries/go_bigquerytools"
	errortools "github.com/leapforce-libraries/go_errortools"
	logistics "github.com/leapforce-libraries/go_exactonline_new/logistics"
	types "github.com/leapforce-libraries/go_types"
)

type SalesItemPriceBQ struct {
	ClientID                   string
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

func getSalesItemPriceBQ(c *logistics.SalesItemPrice, clientID string) SalesItemPriceBQ {
	return SalesItemPriceBQ{
		clientID,
		c.ID.String(),
		c.Account.String(),
		c.AccountName,
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.DefaultItemUnit,
		c.DefaultItemUnitDescription,
		c.Division,
		bigquerytools.DateToNullTimestamp(c.EndDate),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		bigquerytools.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NumberOfItemsPerUnit,
		c.Price,
		c.Quantity,
		bigquerytools.DateToNullTimestamp(c.StartDate),
		c.Unit,
		c.UnitDescription,
	}
}

func (client *Client) WriteSalesSalesItemPricesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := client.ExactOnline().LogisticsClient.NewGetSalesSalesItemPricesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		salesItemPrices, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if salesItemPrices == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *salesItemPrices {
			batchRowCount++

			b, err := json.Marshal(getSalesItemPriceBQ(&tl, client.ClientID()))
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

			fmt.Printf("#SalesSalesItemPrices for client %s flushed: %v\n", client.ClientID(), batchRowCount)

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

	fmt.Printf("#SalesSalesItemPrices for client %s: %v\n", client.ClientID(), rowCount)

	return objectHandles, rowCount, SalesItemPriceBQ{}, nil
}
