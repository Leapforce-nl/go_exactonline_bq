package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	purchaseorder "github.com/leapforce-libraries/go_exactonline_new/purchaseorder"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type PurchaseReturn struct {
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
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Remarks                    string
	ReturnDate                 bigquery.NullTimestamp
	ReturnNumber               int32
	Status                     int16
	Supplier                   string
	SupplierAddress            string
	SupplierContact            string
	SupplierContactFullName    string
	TrackingNumber             string
	Warehouse                  string
	WarehouseCode              string
	WarehouseDescription       string
	YourRef                    string
}

func getPurchaseReturn(c *purchaseorder.PurchaseReturn, softwareClientLicenseGuid string) PurchaseReturn {
	t := time.Now()

	return PurchaseReturn{
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		c.Document.String(),
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Remarks,
		go_bigquery.DateToNullTimestamp(c.ReturnDate),
		c.ReturnNumber,
		c.Status,
		c.Supplier.String(),
		c.SupplierAddress.String(),
		c.SupplierContact.String(),
		c.SupplierContactFullName,
		c.TrackingNumber,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
		c.YourRef,
	}
}

func (service *Service) WritePurchaseReturns(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseOrderService().NewGetPurchaseReturnsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		purchaseReturns, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if purchaseReturns == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *purchaseReturns {
			batchRowCount++

			b, err := json.Marshal(getPurchaseReturn(&tl, softwareClientLicenseGuid))
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

			fmt.Printf("#PurchaseReturns flushed: %v\n", batchRowCount)

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

	fmt.Printf("#PurchaseReturns: %v\n", rowCount)

	return objectHandles, rowCount, PurchaseReturn{}, nil
}
