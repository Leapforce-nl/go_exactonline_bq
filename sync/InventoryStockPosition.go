package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	sync "github.com/leapforce-libraries/go_exactonline_new/sync"
	types "github.com/leapforce-libraries/go_types"
)

type InventoryStockPosition struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	Timestamp                  int64
	CurrentStock               float64
	Division                   int32
	FreeStock                  float64
	ID                         string
	ItemCode                   string
	ItemDescription            string
	ItemID                     string
	PlanningIn                 float64
	PlanningOut                float64
	ProjectedStock             float64
	ReorderPoint               float64
	ReservedStock              float64
	UnitCode                   string
	UnitDescription            string
	Warehouse                  string
	WarehouseCode              string
	WarehouseDescription       string
}

func getInventoryStockPosition(c *sync.InventoryStockPosition, softwareClientLicenseGuid string, maxTimestamp *int64) InventoryStockPosition {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return InventoryStockPosition{
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.CurrentStock,
		c.Division,
		c.FreeStock,
		c.ID.String(),
		c.ItemCode,
		c.ItemDescription,
		c.ItemId.String(),
		c.PlanningIn,
		c.PlanningOut,
		c.ProjectedStock,
		c.ReorderPoint,
		c.ReservedStock,
		c.UnitCode,
		c.UnitDescription,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
	}
}

func (service *Service) WriteInventoryStockPositions(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncInventoryStockPositionsCall(&timestamp)

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

			b, err := json.Marshal(getInventoryStockPosition(&tl, softwareClientLicenseGuid, &maxTimestamp))
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

			fmt.Printf("#InventoryStockPositions flushed: %v\n", batchRowCount)

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

	fmt.Printf("#InventoryStockPositions: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
