package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	salesorder "github.com/leapforce-libraries/go_exactonline_new/salesorder"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type PlannedSalesReturn struct {
	SoftwareClientLicenseGuid_       string
	Created_                         time.Time
	Modified_                        time.Time
	PlannedSalesReturnID             string
	Created                          bigquery.NullTimestamp
	Creator                          string
	CreatorFullName                  string
	DeliveredTo                      string
	DeliveredToContactPerson         string
	DeliveredToContactPersonFullName string
	DeliveredToName                  string
	DeliveryAddress                  string
	Description                      string
	Division                         int32
	Document                         string
	DocumentSubject                  string
	Modified                         bigquery.NullTimestamp
	Modifier                         string
	ModifierFullName                 string
	//PlannedSalesReturnLines
	Remarks              string
	ReturnDate           bigquery.NullTimestamp
	ReturnNumber         int32
	Source               int16
	Status               int16
	Warehouse            string
	WarehouseCode        string
	WarehouseDescription string
}

func getPlannedSalesReturn(c *salesorder.PlannedSalesReturn, softwareClientLicenseGuid string) PlannedSalesReturn {
	t := time.Now()

	return PlannedSalesReturn{
		softwareClientLicenseGuid,
		t, t,
		c.PlannedSalesReturnID.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.DeliveredTo.String(),
		c.DeliveredToContactPerson.String(),
		c.DeliveredToContactPersonFullName,
		c.DeliveredToName,
		c.DeliveryAddress.String(),
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentSubject,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		//c.PlannedSalesReturnLines,
		c.Remarks,
		go_bigquery.DateToNullTimestamp(c.ReturnDate),
		c.ReturnNumber,
		c.Source,
		c.Status,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
	}
}

func (service *Service) WritePlannedSalesReturns(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SalesOrderService().NewGetPlannedSalesReturnsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		plannedSalesReturns, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if plannedSalesReturns == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *plannedSalesReturns {
			batchRowCount++

			b, err := json.Marshal(getPlannedSalesReturn(&tl, softwareClientLicenseGuid))
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

			fmt.Printf("#PlannedSalesReturns flushed: %v\n", batchRowCount)

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

	fmt.Printf("#PlannedSalesReturns: %v\n", rowCount)

	return objectHandles, rowCount, PlannedSalesReturn{}, nil
}
