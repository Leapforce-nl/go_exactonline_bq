package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	purchaseorder "github.com/leapforce-libraries/go_exactonline_new/purchaseorder"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type PurchaseReturnLine struct {
	OrganisationID_             int64
	SoftwareClientLicenceID_    int64
	SoftwareClientLicenseGuid_  string
	Created_                    time.Time
	Modified_                   time.Time
	ID                          string
	CreateCredit                bool
	Created                     bigquery.NullTimestamp
	Creator                     string
	CreatorFullName             string
	Division                    int32
	EntryID                     string
	GoodsReceiptLineID          string
	Item                        string
	ItemCode                    string
	ItemDescription             string
	LineNumber                  int32
	Location                    string
	LocationCode                string
	LocationDescription         string
	Modified                    bigquery.NullTimestamp
	Modifier                    string
	ModifierFullName            string
	Notes                       string
	PurchaseOrderLineID         string
	PurchaseOrderNumber         int32
	ReceiptNumber               int32
	ReceivedQuantity            float64
	ReturnQuantity              float64
	ReturnReasonCodeDescription string
	ReturnReasonCodeID          string
	SupplierItemCode            string
	UnitCode                    string
}

func getPurchaseReturnLine(c *purchaseorder.PurchaseReturnLine, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) PurchaseReturnLine {
	t := time.Now()

	return PurchaseReturnLine{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		c.CreateCredit,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.EntryID.String(),
		c.GoodsReceiptLineID.String(),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.LineNumber,
		c.Location.String(),
		c.LocationCode,
		c.LocationDescription,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.PurchaseOrderLineID.String(),
		c.PurchaseOrderNumber,
		c.ReceiptNumber,
		c.ReceivedQuantity,
		c.ReturnQuantity,
		c.ReturnReasonCodeDescription,
		c.ReturnReasonCodeID.String(),
		c.SupplierItemCode,
		c.UnitCode,
	}
}

func (service *Service) WritePurchaseReturnLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseOrderService().NewGetPurchaseReturnLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		purchaseReturnLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if purchaseReturnLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *purchaseReturnLines {
			batchRowCount++

			b, err := json.Marshal(getPurchaseReturnLine(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
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

			fmt.Printf("#PurchaseReturnLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#PurchaseReturnLines: %v\n", rowCount)

	return objectHandles, rowCount, PurchaseReturnLine{}, nil
}
