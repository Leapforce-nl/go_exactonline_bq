package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"

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
	Timestamp                      int64
	ID                             string
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
	Created                        bigquery.NullTimestamp
	Creator                        string
	CreatorFullName                string
	Currency                       string
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
	InvoiceStatus                  int16
	InvoiceStatusDescription       string
	InvoiceTo                      string
	InvoiceToContactPerson         string
	InvoiceToContactPersonFullName string
	InvoiceToName                  string
	Modified                       bigquery.NullTimestamp
	Modifier                       string
	ModifierFullName               string
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
	Remarks                        string
	Salesperson                    string
	SalespersonFullName            string
	SelectionCode                  string
	SelectionCodeCode              string
	SelectionCodeDescription       string
	ShippingMethod                 string
	ShippingMethodDescription      string
	Status                         int16
	StatusDescription              string
	TaxSchedule                    string
	TaxScheduleCode                string
	TaxScheduleDescription         string
	WarehouseCode                  string
	WarehouseDescription           string
	WarehouseID                    string
	YourRef                        string
}

func getSalesOrderSalesOrder(c *sync.SalesOrderSalesOrder, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) SalesOrderSalesOrder {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	return SalesOrderSalesOrder{
		organisationID,
		softwareClientLicenceID,
		timestamp,
		c.ID.String(),
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
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
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
		c.InvoiceStatus,
		c.InvoiceStatusDescription,
		c.InvoiceTo.String(),
		c.InvoiceToContactPerson.String(),
		c.InvoiceToContactPersonFullName,
		c.InvoiceToName,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
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
		c.Remarks,
		//c.SalesOrderLines,
		c.Salesperson.String(),
		c.SalespersonFullName,
		c.SelectionCode.String(),
		c.SelectionCodeCode,
		c.SelectionCodeDescription,
		c.ShippingMethod.String(),
		c.ShippingMethodDescription,
		c.Status,
		c.StatusDescription,
		c.TaxSchedule.String(),
		c.TaxScheduleCode,
		c.TaxScheduleDescription,
		c.WarehouseCode,
		c.WarehouseDescription,
		c.WarehouseID.String(),
		c.YourRef,
	}
}

func (service *Service) WriteSalesOrderSalesOrders(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
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

	for true {
		transactionLines, e := call.Do()
		if e != nil {
			return nil, nil, e
		}

		if transactionLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *transactionLines {
			batchRowCount++

			b, err := json.Marshal(getSalesOrderSalesOrder(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
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
