package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	sync "github.com/leapforce-libraries/go_exactonline_new/sync"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type SalesOrderSalesOrder struct {
	OrganisationID_                int64
	SoftwareClientLicenceID_       int64
	SoftwareClientLicenseGuid_     string
	Created_                       time.Time
	Modified_                      time.Time
	Timestamp                      int64
	AmountDC                       float64
	AmountDiscount                 float64
	AmountDiscountExclVat          float64
	AmountFC                       float64
	AmountFCExclVat                float64
	ApprovalStatus                 int16
	ApprovalStatusDescription      string
	Approved                       bigquery.NullTimestamp
	Approver                       string
	ApproverFullName               string
	CostCenter                     string  `json:"CostCenter"`
	CostCenterDescription          string  `json:"CostCenterDescription"`
	CostPriceFC                    float64 `json:"CostPriceFC"`
	CostUnit                       string  `json:"CostUnit"`
	CostUnitDescription            string  `json:"CostUnitDescription"`
	Created                        bigquery.NullTimestamp
	Creator                        string
	CreatorFullName                string
	Currency                       string
	CustomerItemCode               string `json:"CustomerItemCode"`
	DeliverTo                      string
	DeliverToContactPerson         string
	DeliverToContactPersonFullName string
	DeliverToName                  string
	DeliveryAddress                string
	DeliveryDate                   bigquery.NullTimestamp
	DeliveryStatus                 int16
	DeliveryStatusDescription      string
	Description                    string
	Discount                       float64
	Division                       int32
	Document                       string
	DocumentNumber                 int32
	DocumentSubject                string
	ID                             string
	InvoiceStatus                  int16
	InvoiceStatusDescription       string
	InvoiceTo                      string
	InvoiceToContactPerson         string
	InvoiceToContactPersonFullName string
	InvoiceToName                  string
	Item                           string
	ItemCode                       string
	ItemDescription                string
	ItemVersion                    string
	ItemVersionDescription         string
	LineNumber                     int32
	Margin                         float64
	Modified                       bigquery.NullTimestamp
	Modifier                       string
	ModifierFullName               string
	NetPrice                       float64
	Notes                          string
	OrderDate                      bigquery.NullTimestamp
	OrderedBy                      string
	OrderedByContactPerson         string
	OrderedByContactPersonFullName string
	OrderedByName                  string
	OrderID                        string
	OrderNumber                    int32
	PaymentCondition               string
	PaymentConditionDescription    string
	PaymentReference               string
	Pricelist                      string
	PricelistDescription           string
	Project                        string
	ProjectDescription             string
	PurchaseOrder                  string
	PurchaseOrderLine              string
	PurchaseOrderLineNumber        int32
	PurchaseOrderNumber            int32
	Quantity                       float64
	QuantityDelivered              float64
	QuantityInvoiced               float64
	Remarks                        string
	SalesPerson                    string
	SalesPersonFullName            string
	SelectionCode                  string
	SelectionCodeCode              string
	SelectionCodeDescription       string
	ShippingMethod                 string
	ShippingMethodDescription      string
	ShopOrder                      string
	Status                         int16
	StatusDescription              string
	TaxSchedule                    string
	TaxScheduleCode                string
	TaxScheduleDescription         string
	UnitCode                       string
	UnitDescription                string
	UnitPrice                      float64
	UseDropShipment                byte
	VATAmount                      float64
	VATCode                        string
	VATCodeDescription             string
	VATPercentage                  float64
	WarehouseCode                  string
	WarehouseDescription           string
	WarehouseID                    string
	YourRef                        string
}

func getSalesOrderSalesOrder(c *sync.SalesOrderSalesOrder, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, maxTimestamp *int64) SalesOrderSalesOrder {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return SalesOrderSalesOrder{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.AmountDC,
		c.AmountDiscount,
		c.AmountDiscountExclVat,
		c.AmountFC,
		c.AmountFCExclVat,
		c.ApprovalStatus,
		c.ApprovalStatusDescription,
		go_bigquery.DateToNullTimestamp(c.Approved),
		c.Approver.String(),
		c.ApproverFullName,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostPriceFC,
		c.CostUnit,
		c.CostUnitDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.CustomerItemCode,
		c.DeliverTo.String(),
		c.DeliverToContactPerson.String(),
		c.DeliverToContactPersonFullName,
		c.DeliverToName,
		c.DeliveryAddress.String(),
		go_bigquery.DateToNullTimestamp(c.DeliveryDate),
		c.DeliveryStatus,
		c.DeliveryStatusDescription,
		c.Description,
		c.Discount,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		c.ID.String(),
		c.InvoiceStatus,
		c.InvoiceStatusDescription,
		c.InvoiceTo.String(),
		c.InvoiceToContactPerson.String(),
		c.InvoiceToContactPersonFullName,
		c.InvoiceToName,
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.ItemVersion.String(),
		c.ItemVersionDescription,
		c.LineNumber,
		c.Margin,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NetPrice,
		c.Notes,
		go_bigquery.DateToNullTimestamp(c.OrderDate),
		c.OrderedBy.String(),
		c.OrderedByContactPerson.String(),
		c.OrderedByContactPersonFullName,
		c.OrderedByName,
		c.OrderID.String(),
		c.OrderNumber,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.PaymentReference,
		c.Pricelist.String(),
		c.PricelistDescription,
		c.Project.String(),
		c.ProjectDescription,
		c.PurchaseOrder.String(),
		c.PurchaseOrderLine.String(),
		c.PurchaseOrderLineNumber,
		c.PurchaseOrderNumber,
		c.Quantity,
		c.QuantityDelivered,
		c.QuantityInvoiced,
		c.Remarks,
		c.SalesPerson.String(),
		c.SalesPersonFullName,
		c.SelectionCode.String(),
		c.SelectionCodeCode,
		c.SelectionCodeDescription,
		c.ShippingMethod.String(),
		c.ShippingMethodDescription,
		c.ShopOrder.String(),
		c.Status,
		c.StatusDescription,
		c.TaxSchedule.String(),
		c.TaxScheduleCode,
		c.TaxScheduleDescription,
		c.UnitCode,
		c.UnitDescription,
		c.UnitPrice,
		c.UseDropShipment,
		c.VATAmount,
		c.VATCode,
		c.VATCodeDescription,
		c.VATPercentage,
		c.WarehouseCode,
		c.WarehouseDescription,
		c.WarehouseID.String(),
		c.YourRef,
	}
}

func (service *Service) WriteSalesOrderSalesOrders(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncSalesOrderSalesOrdersCall(&timestamp)

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

			b, err := json.Marshal(getSalesOrderSalesOrder(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid, &maxTimestamp))
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

			fmt.Printf("#SalesOrderSalesOrders flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesOrderSalesOrders: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
