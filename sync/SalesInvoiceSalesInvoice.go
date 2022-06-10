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

type SalesInvoiceSalesInvoice struct {
	OrganisationID_                      int64
	SoftwareClientLicenceID_             int64
	Created_                             time.Time
	Modified_                            time.Time
	Timestamp                            int64
	ID                                   string
	AmountDC                             float64
	AmountDiscount                       float64
	AmountDiscountExclVat                float64
	AmountFC                             float64
	AmountFCExclVat                      float64
	CostCenter                           string
	CostCenterDescription                string
	CostUnit                             string
	CostUnitDescription                  string
	Created                              bigquery.NullTimestamp
	Creator                              string
	CreatorFullName                      string
	Currency                             string
	CustomerItemCode                     string
	DeliverTo                            string
	DeliverToAddress                     string
	DeliverToContactPerson               string
	DeliverToContactPersonFullName       string
	DeliverToName                        string
	Description                          string
	Discount                             float64
	DiscountType                         int16
	Division                             int32
	Document                             string
	DocumentNumber                       int32
	DocumentSubject                      string
	DueDate                              bigquery.NullTimestamp
	Employee                             string
	EmployeeFullName                     string
	EndTime                              bigquery.NullTimestamp
	ExtraDutyAmountFC                    float64
	ExtraDutyPercentage                  float64
	GAccountAmountFC                     float64
	GLAccount                            string
	GLAccountDescription                 string
	InvoiceDate                          bigquery.NullTimestamp
	InvoiceID                            string
	InvoiceNumber                        int32
	InvoiceTo                            string
	InvoiceToContactPerson               string
	InvoiceToContactPersonFullName       string
	InvoiceToName                        string
	IsExtraDuty                          bool
	Item                                 string
	ItemCode                             string
	ItemDescription                      string
	Journal                              string
	JournalDescription                   string
	LineNumber                           int32
	Modified                             bigquery.NullTimestamp
	Modifier                             string
	ModifierFullName                     string
	NetPrice                             float64
	OrderDate                            bigquery.NullTimestamp
	OrderedBy                            string
	OrderedByContactPerson               string
	OrderedByContactPersonFullName       string
	OrderedByName                        string
	OrderNumber                          int32
	PaymentCondition                     string
	PaymentConditionDescription          string
	PaymentReference                     string
	Pricelist                            string
	PricelistDescription                 string
	Project                              string
	ProjectDescription                   string
	ProjectWBS                           string
	ProjectWBSDescription                string
	Quantity                             float64
	Remarks                              string
	SalesOrder                           string
	SalesOrderLine                       string
	SalesOrderLineNumber                 int32
	SalesOrderNumber                     int32
	SalesPerson                          string
	SalesPersonFullName                  string
	StarterSalesInvoiceStatus            int16
	StarterSalesInvoiceStatusDescription string
	Status                               int16
	StatusDescription                    string
	TaxSchedule                          string
	TaxScheduleCode                      string
	TaxScheduleDescription               string
	Type                                 int32
	TypeDescription                      string
	UnitCode                             string
	UnitDescription                      string
	UnitPrice                            float64
	VATAmountDC                          float64
	VATAmountFC                          float64
	Warehouse                            string
	WithholdingTaxAmountFC               float64
	WithholdingTaxBaseAmount             float64
	WithholdingTaxPercentage             float64
	YourRef                              string
}

func getSalesInvoiceSalesInvoice(c *sync.SalesInvoiceSalesInvoice, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) SalesInvoiceSalesInvoice {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return SalesInvoiceSalesInvoice{
		organisationID,
		softwareClientLicenceID,
		t, t,
		timestamp,
		c.ID.String(),
		c.AmountDC,
		c.AmountDiscount,
		c.AmountDiscountExclVat,
		c.AmountFC,
		c.AmountFCExclVat,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostUnit,
		c.CostUnitDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.CustomerItemCode,
		c.DeliverTo.String(),
		c.DeliverToAddress.String(),
		c.DeliverToContactPerson.String(),
		c.DeliverToContactPersonFullName,
		c.DeliverToName,
		c.Description,
		c.Discount,
		c.DiscountType,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		go_bigquery.DateToNullTimestamp(c.DueDate),
		c.Employee.String(),
		c.EmployeeFullName,
		go_bigquery.DateToNullTimestamp(c.EndTime),
		c.ExtraDutyAmountFC,
		c.ExtraDutyPercentage,
		c.GAccountAmountFC,
		c.GLAccount.String(),
		c.GLAccountDescription,
		go_bigquery.DateToNullTimestamp(c.InvoiceDate),
		c.InvoiceID.String(),
		c.InvoiceNumber,
		c.InvoiceTo.String(),
		c.InvoiceToContactPerson.String(),
		c.InvoiceToContactPersonFullName,
		c.InvoiceToName,
		c.IsExtraDuty,
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.Journal,
		c.JournalDescription,
		c.LineNumber,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NetPrice,
		go_bigquery.DateToNullTimestamp(c.OrderDate),
		c.OrderedBy.String(),
		c.OrderedByContactPerson.String(),
		c.OrderedByContactPersonFullName,
		c.OrderedByName,
		c.OrderNumber,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.PaymentReference,
		c.Pricelist.String(),
		c.PricelistDescription,
		c.Project.String(),
		c.ProjectDescription,
		c.ProjectWBS.String(),
		c.ProjectWBSDescription,
		c.Quantity,
		c.Remarks,
		c.SalesOrder.String(),
		c.SalesOrderLine.String(),
		c.SalesOrderLineNumber,
		c.SalesOrderNumber,
		c.SalesPerson.String(),
		c.SalesPersonFullName,
		c.StarterSalesInvoiceStatus,
		c.StarterSalesInvoiceStatusDescription,
		c.Status,
		c.StatusDescription,
		c.TaxSchedule.String(),
		c.TaxScheduleCode,
		c.TaxScheduleDescription,
		c.Type,
		c.TypeDescription,
		c.UnitCode,
		c.UnitDescription,
		c.UnitPrice,
		c.VATAmountDC,
		c.VATAmountFC,
		c.Warehouse.String(),
		c.WithholdingTaxAmountFC,
		c.WithholdingTaxBaseAmount,
		c.WithholdingTaxPercentage,
		c.YourRef,
	}
}

func (service *Service) WriteSalesInvoiceSalesInvoices(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncSalesInvoiceSalesInvoicesCall(&timestamp)

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

			b, err := json.Marshal(getSalesInvoiceSalesInvoice(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
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

			fmt.Printf("#SalesInvoiceSalesInvoices flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesInvoiceSalesInvoices: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
