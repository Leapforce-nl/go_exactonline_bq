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

type SalesOrderSalesOrderHeader struct {
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
	ID                             string
	IncotermAddress                string
	IncotermCode                   string
	IncotermVersion                int16
	InvoiceStatus                  int16
	InvoiceStatusDescription       string
	InvoiceTo                      string
	InvoiceToContactPerson         string
	InvoiceToContactPersonFullName string
	InvoiceToName                  string
	Modified                       bigquery.NullTimestamp
	Modifier                       string
	ModifierFullName               string
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
	Project                        string
	ProjectCode                    string
	ProjectDescription             string
	Remarks                        string
	SalesChannel                   string
	SalesChannelCode               string
	SalesChannelDescription        string
	Salesperson                    string
	SalespersonFullName            string
	SelectionCode                  string
	SelectionCodeCode              string
	SelectionCodeDescription       string
	ShippingMethod                 string
	ShippingMethodCode             string
	ShippingMethodDescription      string
	Status                         int16
	StatusDescription              string
	VATAmount                      float64
	VATCode                        string
	VATCodeDescription             string
	WarehouseCode                  string
	WarehouseDescription           string
	WarehouseID                    string
	YourRef                        string
}

func getSalesOrderSalesOrderHeader(c *sync.SalesOrderSalesOrderHeader, softwareClientLicenseGuid string, maxTimestamp *int64) SalesOrderSalesOrderHeader {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return SalesOrderSalesOrderHeader{
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.AmountDc,
		c.AmountDiscount,
		c.AmountDiscountExclVat,
		c.AmountFc,
		c.AmountFcExclVat,
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
		c.Id.String(),
		c.IncotermAddress,
		c.IncotermCode,
		c.IncotermVersion,
		c.InvoiceStatus,
		c.InvoiceStatusDescription,
		c.InvoiceTo.String(),
		c.InvoiceToContactPerson.String(),
		c.InvoiceToContactPersonFullName,
		c.InvoiceToName,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		go_bigquery.DateToNullTimestamp(c.OrderDate),
		c.OrderedBy.String(),
		c.OrderedByContactPerson.String(),
		c.OrderedByContactPersonFullName,
		c.OrderedByName,
		c.OrderId.String(),
		c.OrderNumber,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.PaymentReference,
		c.Project.String(),
		c.ProjectCode,
		c.ProjectDescription,
		c.Remarks,
		c.SalesChannel.String(),
		c.SalesChannelCode,
		c.SalesChannelDescription,
		c.Salesperson.String(),
		c.SalespersonFullName,
		c.SelectionCode.String(),
		c.SelectionCodeCode,
		c.SelectionCodeDescription,
		c.ShippingMethod.String(),
		c.ShippingMethodCode,
		c.ShippingMethodDescription,
		c.Status,
		c.StatusDescription,
		c.VatAmount,
		c.VatCode,
		c.VatCodeDescription,
		c.WarehouseCode,
		c.WarehouseDescription,
		c.WarehouseId.String(),
		c.YourRef,
	}
}

func (service *Service) WriteSalesOrderSalesOrderHeaders(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncSalesOrderSalesOrderHeadersCall(&timestamp)

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

			b, err := json.Marshal(getSalesOrderSalesOrderHeader(&tl, softwareClientLicenseGuid, &maxTimestamp))
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

			fmt.Printf("#SalesOrderSalesOrderHeaders flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesOrderSalesOrderHeaders: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
