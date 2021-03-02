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

type PurchaseOrderBQ struct {
	ClientID                      string
	PurchaseOrderID               string
	AmountDC                      float64
	AmountFC                      float64
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
	Division                      int32
	Document                      string
	DocumentSubject               string
	DropShipment                  bool
	ExchangeRate                  float64
	InvoiceStatus                 int32
	Modified                      bigquery.NullTimestamp
	Modifier                      string
	ModifierFullName              string
	OrderDate                     bigquery.NullTimestamp
	OrderNumber                   int32
	OrderStatus                   int32
	PaymentCondition              string
	PaymentConditionDescription   string
	PurchaseAgent                 string
	PurchaseAgentFullName         string
	ReceiptDate                   bigquery.NullTimestamp
	ReceiptStatus                 int32
	Remarks                       string
	SalesOrder                    string
	SalesOrderNumber              int32
	SelectionCode                 string
	SelectionCodeCode             string
	SelectionCodeDescription      string
	ShippingMethod                string
	ShippingMethodDescription     string
	Source                        int16
	Supplier                      string
	SupplierCode                  string
	SupplierContact               string
	SupplierContactPersonFullName string
	SupplierName                  string
	VATAmount                     float64
	Warehouse                     string
	WarehouseCode                 string
	WarehouseDescription          string
	YourRef                       string
}

func getPurchaseOrderBQ(c *purchaseorder.PurchaseOrder, clientID string) PurchaseOrderBQ {
	return PurchaseOrderBQ{
		clientID,
		c.PurchaseOrderID.String(),
		c.AmountDC,
		c.AmountFC,
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
		c.Division,
		c.Document.String(),
		c.DocumentSubject,
		c.DropShipment,
		c.ExchangeRate,
		c.InvoiceStatus,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		go_bigquery.DateToNullTimestamp(c.OrderDate),
		c.OrderNumber,
		c.OrderStatus,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.PurchaseAgent.String(),
		c.PurchaseAgentFullName,
		go_bigquery.DateToNullTimestamp(c.ReceiptDate),
		c.ReceiptStatus,
		c.Remarks,
		c.SalesOrder.String(),
		c.SalesOrderNumber,
		c.SelectionCode.String(),
		c.SelectionCodeCode,
		c.SelectionCodeDescription,
		c.ShippingMethod.String(),
		c.ShippingMethodDescription,
		c.Source,
		c.Supplier.String(),
		c.SupplierCode,
		c.SupplierContact.String(),
		c.SupplierContactPersonFullName,
		c.SupplierName,
		c.VATAmount,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
		c.YourRef,
	}
}

func (service *Service) WritePurchaseOrdersBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseOrderService().NewGetPurchaseOrdersCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		purchaseOrders, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if purchaseOrders == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *purchaseOrders {
			batchRowCount++

			b, err := json.Marshal(getPurchaseOrderBQ(&tl, service.ClientID()))
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

			fmt.Printf("#PurchaseOrders for service %s flushed: %v\n", service.ClientID(), batchRowCount)

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

	fmt.Printf("#PurchaseOrders for service %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, PurchaseOrderBQ{}, nil
}
