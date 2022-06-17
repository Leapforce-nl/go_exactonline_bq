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

type PurchaseOrderLine struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	AmountDC                   float64
	AmountFC                   float64
	CostCenter                 string
	CostCenterDescription      string
	CostUnit                   string
	CostUnitDescription        string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Description                string
	Discount                   float64
	Division                   int32
	Expense                    string
	ExpenseDescription         string
	InStock                    float64
	InvoicedQuantity           float64
	Item                       string
	ItemCode                   string
	ItemDescription            string
	ItemDivisable              bool
	LineNumber                 int32
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	NetPrice                   float64
	Notes                      string
	Project                    string
	ProjectCode                string
	ProjectDescription         string
	ProjectedStock             float64
	PurchaseOrderID            string
	Quantity                   float64
	QuantityInPurchaseUnits    float64
	Rebill                     bool
	ReceiptDate                bigquery.NullTimestamp
	ReceivedQuantity           float64
	SalesOrder                 string
	SalesOrderLine             string
	SalesOrderLineNumber       int32
	SalesOrderNumber           int32
	SupplierItemCode           string
	SupplierItemCopyRemarks    byte
	Unit                       string
	UnitDescription            string
	UnitPrice                  float64
	VATAmount                  float64
	VATCode                    string
	VATDescription             string
	VATPercentage              float64
}

func getPurchaseOrderLine(c *purchaseorder.PurchaseOrderLine, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) PurchaseOrderLine {
	t := time.Now()

	return PurchaseOrderLine{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		c.AmountDC,
		c.AmountFC,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostUnit,
		c.CostUnitDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Discount,
		c.Division,
		c.Expense.String(),
		c.ExpenseDescription,
		c.InStock,
		c.InvoicedQuantity,
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.ItemDivisable,
		c.LineNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NetPrice,
		c.Notes,
		c.Project.String(),
		c.ProjectCode,
		c.ProjectDescription,
		c.ProjectedStock,
		c.PurchaseOrderID.String(),
		c.Quantity,
		c.QuantityInPurchaseUnits,
		c.Rebill,
		go_bigquery.DateToNullTimestamp(c.ReceiptDate),
		c.ReceivedQuantity,
		c.SalesOrder.String(),
		c.SalesOrderLine.String(),
		c.SalesOrderLineNumber,
		c.SalesOrderNumber,
		c.SupplierItemCode,
		c.SupplierItemCopyRemarks,
		c.Unit,
		c.UnitDescription,
		c.UnitPrice,
		c.VATAmount,
		c.VATCode,
		c.VATDescription,
		c.VATPercentage,
	}
}

func (service *Service) WritePurchaseOrderLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseOrderService().NewGetPurchaseOrderLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		purchaseOrderLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if purchaseOrderLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *purchaseOrderLines {
			batchRowCount++

			b, err := json.Marshal(getPurchaseOrderLine(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
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

			fmt.Printf("#PurchaseOrderLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#PurchaseOrderLines: %v\n", rowCount)

	return objectHandles, rowCount, PurchaseOrderLine{}, nil
}
