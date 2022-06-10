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

type StockCount struct {
	OrganisationID_              int64
	SoftwareClientLicenceID_     int64
	Created_                     time.Time
	Modified_                    time.Time
	StockCountID                 string
	Created                      bigquery.NullTimestamp
	Creator                      string
	CreatorFullName              string
	Description                  string
	Division                     int32
	EntryNumber                  int32
	Modified                     bigquery.NullTimestamp
	Modifier                     string
	ModifierFullName             string
	OffsetGLInventory            string
	OffsetGLInventoryCode        string
	OffsetGLInventoryDescription string
	Source                       int16
	Status                       int16
	StockCountDate               bigquery.NullTimestamp
	StockCountNumber             int32
	Warehouse                    string
	WarehouseCode                string
	WarehouseDescription         string
}

func getStockCount(c *inventory.StockCount, organisationID int64, softwareClientLicenceID int64) StockCount {
	t := time.Now()

	return StockCount{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.StockCountID.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		c.EntryNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.OffsetGLInventory.String(),
		c.OffsetGLInventoryCode,
		c.OffsetGLInventoryDescription,
		c.Source,
		c.Status,
		go_bigquery.DateToNullTimestamp(c.StockCountDate),
		//c.StockCountLines,
		c.StockCountNumber,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
	}
}

func (service *Service) WriteStockCounts(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.InventoryService().NewGetStockCountsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		stockCounts, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if stockCounts == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *stockCounts {
			batchRowCount++

			b, err := json.Marshal(getStockCount(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#StockCounts flushed: %v\n", batchRowCount)

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

	fmt.Printf("#StockCounts: %v\n", rowCount)

	return objectHandles, rowCount, StockCount{}, nil
}
