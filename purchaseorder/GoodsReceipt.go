package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	purchaseorder "github.com/leapforce-libraries/go_exactonline_new/purchaseorder"
	types "github.com/leapforce-libraries/go_types"
)

type GoodsReceipt struct {
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
	Document                   string
	DocumentSubject            string
	EntryNumber                int32
	GoodsReceiptLineCount      int32
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	ReceiptDate                bigquery.NullTimestamp
	ReceiptNumber              int32
	Remarks                    string
	Supplier                   string
	SupplierCode               string
	SupplierContact            string
	SupplierContactFullName    string
	SupplierName               string
	Warehouse                  string
	WarehouseCode              string
	WarehouseDescription       string
	YourRef                    string
}

func getGoodsReceipt(c *purchaseorder.GoodsReceipt, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) GoodsReceipt {
	t := time.Now()

	return GoodsReceipt{
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
		c.Document.String(),
		c.DocumentSubject,
		c.EntryNumber,
		c.GoodsReceiptLineCount,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		go_bigquery.DateToNullTimestamp(c.ReceiptDate),
		c.ReceiptNumber,
		c.Remarks,
		c.Supplier.String(),
		c.SupplierCode,
		c.SupplierContact.String(),
		c.SupplierContactFullName,
		c.SupplierName,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
		c.YourRef,
	}
}

func (service *Service) WriteGoodsReceipts(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseOrderService().NewGetGoodsReceiptsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		goodsReceipts, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if goodsReceipts == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *goodsReceipts {
			batchRowCount++

			b, err := json.Marshal(getGoodsReceipt(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
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

			fmt.Printf("#GoodsReceipts flushed: %v\n", batchRowCount)

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

	fmt.Printf("#GoodsReceipts: %v\n", rowCount)

	return objectHandles, rowCount, GoodsReceipt{}, nil
}
