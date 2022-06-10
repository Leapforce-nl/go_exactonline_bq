package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	inventory "github.com/leapforce-libraries/go_exactonline_new/inventory"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type StockCountLine struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	CostPrice                  float64
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Division                   int32
	Item                       string
	ItemCode                   string
	ItemCostPrice              float64
	ItemDescription            string
	ItemDivisable              bool
	LineNumber                 int32
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	QuantityDifference         float64
	QuantityInStock            float64
	QuantityNew                float64
	StockCountID               string
	StockKeepingUnit           string
	StorageLocation            string
	StorageLocationCode        string
	StorageLocationDescription string
}

func getStockCountLine(c *inventory.StockCountLine, organisationID int64, softwareClientLicenceID int64) StockCountLine {
	t := time.Now()

	return StockCountLine{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		//c.BatchNumbers,
		c.CostPrice,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.Item.String(),
		c.ItemCode,
		c.ItemCostPrice,
		c.ItemDescription,
		c.ItemDivisable,
		c.LineNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.QuantityDifference,
		c.QuantityInStock,
		c.QuantityNew,
		//c.SerialNumbers,
		c.StockCountID.String(),
		c.StockKeepingUnit,
		c.StorageLocation.String(),
		c.StorageLocationCode,
		c.StorageLocationDescription,
	}
}

func (service *Service) WriteStockCountLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.InventoryService().NewGetStockCountLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		stockCountLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if stockCountLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *stockCountLines {
			batchRowCount++

			b, err := json.Marshal(getStockCountLine(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#StockCountLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#StockCountLines: %v\n", rowCount)

	return objectHandles, rowCount, StockCountLine{}, nil
}
