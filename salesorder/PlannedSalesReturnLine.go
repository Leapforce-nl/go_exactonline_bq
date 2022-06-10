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

type PlannedSalesReturnLine struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Created_                 time.Time
	Modified_                time.Time
	ID                       string
	//BatchNumbers
	CreateCredit          byte
	Created               bigquery.NullTimestamp
	Creator               string
	CreatorFullName       string
	Division              int32
	GoodDeliveryLineID    string
	Item                  string
	ItemCode              string
	ItemDescription       string
	LineNumber            int32
	Modified              bigquery.NullTimestamp
	Modifier              string
	ModifierFullName      string
	Notes                 string
	PlannedReturnQuantity float64
	PlannedSalesReturnID  string
	ReceivedQuantity      float64
	SalesOrderLineID      string
	SalesOrderNumber      int32
	//SerialNumbers
	StockTransactionEntryID    string
	StorageLocation            string
	StorageLocationCode        string
	StorageLocationDescription string
	UnitCode                   string
	UnitDescription            string
}

func getPlannedSalesReturnLine(c *salesorder.PlannedSalesReturnLine, organisationID int64, softwareClientLicenceID int64) PlannedSalesReturnLine {
	t := time.Now()

	return PlannedSalesReturnLine{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		//c.BatchNumbers,
		c.CreateCredit,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.GoodDeliveryLineID.String(),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.LineNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.PlannedReturnQuantity,
		c.PlannedSalesReturnID.String(),
		c.ReceivedQuantity,
		c.SalesOrderLineID.String(),
		c.SalesOrderNumber,
		//c.SerialNumbers,
		c.StockTransactionEntryID.String(),
		c.StorageLocation.String(),
		c.StorageLocationCode,
		c.StorageLocationDescription,
		c.UnitCode,
		c.UnitDescription,
	}
}

func (service *Service) WritePlannedSalesReturnLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SalesOrderService().NewGetPlannedSalesReturnLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		plannedSalesReturnLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if plannedSalesReturnLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *plannedSalesReturnLines {
			batchRowCount++

			b, err := json.Marshal(getPlannedSalesReturnLine(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#PlannedSalesReturnLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#PlannedSalesReturnLines: %v\n", rowCount)

	return objectHandles, rowCount, PlannedSalesReturnLine{}, nil
}
