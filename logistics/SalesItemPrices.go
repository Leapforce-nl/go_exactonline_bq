package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	_bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	logistics "github.com/leapforce-libraries/go_exactonline_new/logistics"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type SalesItemPriceBQ struct {
	ClientID                   string
	ID                         string
	Account                    string
	AccountName                string
	Created                    _bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Currency                   string
	DefaultItemUnit            string
	DefaultItemUnitDescription string
	Division                   int32
	EndDate                    _bigquery.NullTimestamp
	Item                       string
	ItemCode                   string
	ItemDescription            string
	Modified                   _bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	NumberOfItemsPerUnit       float64
	Price                      float64
	Quantity                   float64
	StartDate                  _bigquery.NullTimestamp
	Unit                       string
	UnitDescription            string
}

func getSalesItemPriceBQ(c *logistics.SalesItemPrice, clientID string) SalesItemPriceBQ {
	return SalesItemPriceBQ{
		clientID,
		c.ID.String(),
		c.Account.String(),
		c.AccountName,
		bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.DefaultItemUnit,
		c.DefaultItemUnitDescription,
		c.Division,
		bigquery.DateToNullTimestamp(c.EndDate),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NumberOfItemsPerUnit,
		c.Price,
		c.Quantity,
		bigquery.DateToNullTimestamp(c.StartDate),
		c.Unit,
		c.UnitDescription,
	}
}

func (service *Service) WriteSalesItemPricesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.LogisticsService().NewGetSalesItemPricesCall(lastModified)

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

			b, err := json.Marshal(getSalesItemPriceBQ(&tl, service.ClientID()))
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

			fmt.Printf("#SalesItemPrices for service %s flushed: %v\n", service.ClientID(), batchRowCount)

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

	fmt.Printf("#SalesItemPrices for service %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, SalesItemPriceBQ{}, nil
}
