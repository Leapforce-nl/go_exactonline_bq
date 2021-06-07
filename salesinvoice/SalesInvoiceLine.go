package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	salesinvoice "github.com/leapforce-libraries/go_exactonline_new/salesinvoice"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type SalesInvoiceLine struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	ID                       string
	AmountDC                 float64
	AmountFC                 float64
	CostCenter               string
	CostCenterDescription    string
	CostUnit                 string
	CostUnitDescription      string
	CustomerItemCode         string
	DeliveryDate             bigquery.NullTimestamp
	Description              string
	Discount                 float64
	Division                 int32
	Employee                 string
	EmployeeFullName         string
	EndTime                  bigquery.NullTimestamp
	ExtraDutyAmountFC        float64
	ExtraDutyPercentage      float64
	GLAccount                string
	GLAccountDescription     string
	InvoiceID                string
	Item                     string
	ItemCode                 string
	ItemDescription          string
	LineNumber               int32
	NetPrice                 float64
	Notes                    string
	Pricelist                string
	PricelistDescription     string
	Project                  string
	ProjectDescription       string
	ProjectWBS               string
	ProjectWBSDescription    string
	Quantity                 float64
	SalesOrder               string
	SalesOrderLine           string
	SalesOrderLineNumber     int32
	SalesOrderNumber         int32
	StartTime                bigquery.NullTimestamp
	Subscription             string
	SubscriptionDescription  string
	TaxSchedule              string
	TaxScheduleCode          string
	TaxScheduleDescription   string
	UnitCode                 string
	UnitDescription          string
	UnitPrice                float64
	VATAmountDC              float64
	VATAmountFC              float64
	VATCode                  string
	VATCodeDescription       string
	VATPercentage            float64
}

func getSalesInvoiceLine(c *salesinvoice.SalesInvoiceLine, organisationID int64, softwareClientLicenceID int64) SalesInvoiceLine {
	return SalesInvoiceLine{
		organisationID,
		softwareClientLicenceID,
		c.ID.String(),
		c.AmountDC,
		c.AmountFC,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostUnit,
		c.CostUnitDescription,
		c.CustomerItemCode,
		go_bigquery.DateToNullTimestamp(c.DeliveryDate),
		c.Description,
		c.Discount,
		c.Division,
		c.Employee.String(),
		c.EmployeeFullName,
		go_bigquery.DateToNullTimestamp(c.EndTime),
		c.ExtraDutyAmountFC,
		c.ExtraDutyPercentage,
		c.GLAccount.String(),
		c.GLAccountDescription,
		c.InvoiceID.String(),
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.LineNumber,
		c.NetPrice,
		c.Notes,
		c.Pricelist.String(),
		c.PricelistDescription,
		c.Project.String(),
		c.ProjectDescription,
		c.ProjectWBS.String(),
		c.ProjectWBSDescription,
		c.Quantity,
		c.SalesOrder.String(),
		c.SalesOrderLine.String(),
		c.SalesOrderLineNumber,
		c.SalesOrderNumber,
		go_bigquery.DateToNullTimestamp(c.StartTime),
		c.Subscription.String(),
		c.SubscriptionDescription,
		c.TaxSchedule.String(),
		c.TaxScheduleCode,
		c.TaxScheduleDescription,
		c.UnitCode,
		c.UnitDescription,
		c.UnitPrice,
		c.VATAmountDC,
		c.VATAmountFC,
		c.VATCode,
		c.VATCodeDescription,
		c.VATPercentage,
	}
}

func (service *Service) WriteSalesInvoiceLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SalesInvoiceService().NewGetSalesInvoiceLinesCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		salesInvoiceLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if salesInvoiceLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *salesInvoiceLines {
			batchRowCount++

			b, err := json.Marshal(getSalesInvoiceLine(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#SalesInvoiceLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesInvoiceLines: %v\n", rowCount)

	return objectHandles, rowCount, SalesInvoiceLine{}, nil
}
