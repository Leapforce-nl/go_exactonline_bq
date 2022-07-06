package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	purchaseorder "github.com/leapforce-libraries/go_exactonline_new/purchaseorder"
	types "github.com/leapforce-libraries/go_types"
)

type GoodsReceiptLine struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Description                string
	Division                   int32
	GoodsReceiptID             string
	Item                       string
	ItemCode                   string
	ItemDescription            string
	ItemUnitCode               string
	LineNumber                 int32
	Location                   string
	LocationCode               string
	LocationDescription        string
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Notes                      string
	Project                    string
	ProjectCode                string
	ProjectDescription         string
	PurchaseOrderID            string
	PurchaseOrderLineID        string
	PurchaseOrderNumber        int32
	QuantityOrdered            float64
	QuantityReceived           float64
	SupplierItemCode           string
}

func getGoodsReceiptLine(c *purchaseorder.GoodsReceiptLine, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) GoodsReceiptLine {
	t := time.Now()

	return GoodsReceiptLine{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		c.GoodsReceiptID.String(),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.ItemUnitCode,
		c.LineNumber,
		c.Location.String(),
		c.LocationCode,
		c.LocationDescription,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.Project.String(),
		c.ProjectCode,
		c.ProjectDescription,
		c.PurchaseOrderID.String(),
		c.PurchaseOrderLineID.String(),
		c.PurchaseOrderNumber,
		c.QuantityOrdered,
		c.QuantityReceived,
		c.SupplierItemCode,
	}
}

func (service *Service) WriteGoodsReceiptLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseOrderService().NewGetGoodsReceiptLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		goodsReceiptLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if goodsReceiptLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *goodsReceiptLines {
			batchRowCount++

			b, err := json.Marshal(getGoodsReceiptLine(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
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

			fmt.Printf("#GoodsReceiptLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#GoodsReceiptLines: %v\n", rowCount)

	return objectHandles, rowCount, GoodsReceiptLine{}, nil
}
