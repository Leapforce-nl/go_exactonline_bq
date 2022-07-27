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

type PurchaseOrderPurchaseOrder struct {
	SoftwareClientLicenseGuid_    string
	Created_                      time.Time
	Modified_                     time.Time
	Timestamp                     int64
	AmountDC                      float64
	AmountFC                      float64
	CostCenter                    string
	CostCenterCode                string
	CostCenterDescription         string
	CostUnit                      string
	CostUnitCode                  string
	CostUnitDescription           string
	Created                       bigquery.NullTimestamp
	Creator                       string
	CreatorFullName               string
	Currency                      string
	DeliveryAccount               string
	DeliveryAccountCode           string
	DeliveryAccountName           string
	DeliveryAddress               string
	DeliveryContact               string
	DeliveryContactPersonFullName string
	Description                   string
	Discount                      float64
	Division                      int32
	Document                      string
	DocumentNumber                int32
	DocumentSubject               string
	DropShipment                  bool
	ExchangeRate                  float64
	Expense                       string
	ExpenseDescription            string
	ID                            string
	IncotermAddress               string
	IncotermCode                  string
	IncotermVersion               int16

	InvoicedQuantity              float64
	InvoiceStatus                 int32
	IsBatchNumberItem             byte
	IsSerialNumberItem            byte
	Item                          string
	ItemBarcode                   string
	ItemCode                      string
	ItemDescription               string
	ItemDivisable                 bool
	LineNumber                    int32
	Modified                      bigquery.NullTimestamp
	Modifier                      string
	ModifierFullName              string
	NetPrice                      float64
	Notes                         string
	OrderDate                     bigquery.NullTimestamp
	OrderNumber                   int32
	OrderStatus                   int32
	PaymentCondition              string
	PaymentConditionDescription   string
	Project                       string
	ProjectCode                   string
	ProjectDescription            string
	PurchaseAgent                 string
	PurchaseAgentFullName         string
	PurchaseOrderID               string
	Quantity                      float64
	QuantityInPurchaseUnits       float64
	Rebill                        bool
	ReceiptDate                   bigquery.NullTimestamp
	ReceiptStatus                 int32
	ReceivedQuantity              float64
	Remarks                       string
	SalesOrder                    string
	SalesOrderLine                string
	SalesOrderLineNumber          int32
	SalesOrderNumber              int32
	SelectionCode                 string
	SelectionCodeCode             string
	SelectionCodeDescription      string
	SendingMethod                 int32
	ShippingMethod                string
	ShippingMethodCode            string
	ShippingMethodDescription     string
	Source                        int16
	Supplier                      string
	SupplierCode                  string
	SupplierContact               string
	SupplierContactPersonFullName string
	SupplierItemCode              string
	SupplierItemCopyRemarks       byte
	SupplierName                  string
	Unit                          string
	UnitDescription               string
	UnitPrice                     float64
	VATAmount                     float64
	VATCode                       string
	VATDescription                string
	VATPercentage                 float64
	Warehouse                     string
	WarehouseCode                 string
	WarehouseDescription          string
	YourRef                       string
}

func getPurchaseOrderPurchaseOrder(c *sync.PurchaseOrderPurchaseOrder, softwareClientLicenseGuid string, maxTimestamp *int64) PurchaseOrderPurchaseOrder {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return PurchaseOrderPurchaseOrder{
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.AmountDC,
		c.AmountFC,
		c.CostCenter.String(),
		c.CostCenterCode,
		c.CostCenterDescription,
		c.CostUnit.String(),
		c.CostUnitCode,
		c.CostUnitDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.DeliveryAccount.String(),
		c.DeliveryAccountCode,
		c.DeliveryAccountName,
		c.DeliveryAddress.String(),
		c.DeliveryContact.String(),
		c.DeliveryContactPersonFullName,
		c.Description,
		c.Discount,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		c.DropShipment,
		c.ExchangeRate,
		c.Expense.String(),
		c.ExpenseDescription,
		c.ID.String(),
		c.IncotermAddress,
		c.IncotermCode,
		c.IncotermVersion,
		c.InvoicedQuantity,
		c.InvoiceStatus,
		c.IsBatchNumberItem,
		c.IsSerialNumberItem,
		c.Item.String(),
		c.ItemBarcode,
		c.ItemCode,
		c.ItemDescription,
		c.ItemDivisable,
		c.LineNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NetPrice,
		c.Notes,
		go_bigquery.DateToNullTimestamp(c.OrderDate),
		c.OrderNumber,
		c.OrderStatus,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.Project.String(),
		c.ProjectCode,
		c.ProjectDescription,
		c.PurchaseAgent.String(),
		c.PurchaseAgentFullName,
		c.PurchaseOrderID.String(),
		c.Quantity,
		c.QuantityInPurchaseUnits,
		c.Rebill,
		go_bigquery.DateToNullTimestamp(c.ReceiptDate),
		c.ReceiptStatus,
		c.ReceivedQuantity,
		c.Remarks,
		c.SalesOrder.String(),
		c.SalesOrderLine.String(),
		c.SalesOrderLineNumber,
		c.SalesOrderNumber,
		c.SelectionCode.String(),
		c.SelectionCodeCode,
		c.SelectionCodeDescription,
		c.SendingMethod,
		c.ShippingMethod.String(),
		c.ShippingMethodCode,
		c.ShippingMethodDescription,
		c.Source,
		c.Supplier.String(),
		c.SupplierCode,
		c.SupplierContact.String(),
		c.SupplierContactPersonFullName,
		c.SupplierItemCode,
		c.SupplierItemCopyRemarks,
		c.SupplierName,
		c.Unit,
		c.UnitDescription,
		c.UnitPrice,
		c.VATAmount,
		c.VATCode,
		c.VATDescription,
		c.VATPercentage,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
		c.YourRef,
	}
}

func (service *Service) WritePurchaseOrderPurchaseOrders(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncPurchaseOrderPurchaseOrdersCall(&timestamp)

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

			b, err := json.Marshal(getPurchaseOrderPurchaseOrder(&tl, softwareClientLicenseGuid, &maxTimestamp))
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

			fmt.Printf("#PurchaseOrderPurchaseOrders flushed: %v\n", batchRowCount)

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

	fmt.Printf("#PurchaseOrderPurchaseOrders: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
