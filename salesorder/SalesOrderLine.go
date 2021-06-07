package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	salesorder "github.com/leapforce-libraries/go_exactonline_new/salesorder"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type SalesOrderLine struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	ID                       string
	AmountDC                 float64
	AmountFC                 float64
	CostCenter               string
	CostCenterDescription    string
	CostPriceFC              float64
	CostUnit                 string
	CostUnitDescription      string
	CustomerItemCode         string
	DeliveryDate             bigquery.NullTimestamp
	Description              string
	Discount                 float64
	Division                 int32
	Item                     string
	ItemCode                 string
	ItemDescription          string
	ItemVersion              string
	ItemVersionDescription   string
	LineNumber               int32
	NetPrice                 float64
	Notes                    string
	OrderID                  string
	OrderNumber              int32
	Pricelist                string
	PricelistDescription     string
	Project                  string
	ProjectDescription       string
	PurchaseOrder            string
	PurchaseOrderLine        string
	PurchaseOrderLineNumber  int32
	PurchaseOrderNumber      int32
	Quantity                 float64
	ShopOrder                string
	UnitCode                 string
	UnitDescription          string
	UnitPrice                float64
	UseDropShipment          byte
	VATAmount                float64
	VATCode                  string
	VATCodeDescription       string
	VATPercentage            float64
}

func getSalesOrderLine(c *salesorder.SalesOrderLine, organisationID int64, softwareClientLicenceID int64) SalesOrderLine {
	return SalesOrderLine{
		organisationID,
		softwareClientLicenceID,
		c.ID.String(),
		c.AmountDC,
		c.AmountFC,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostPriceFC,
		c.CostUnit,
		c.CostUnitDescription,
		c.CustomerItemCode,
		go_bigquery.DateToNullTimestamp(c.DeliveryDate),
		c.Description,
		c.Discount,
		c.Division,
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.ItemVersion.String(),
		c.ItemVersionDescription,
		c.LineNumber,
		c.NetPrice,
		c.Notes,
		c.OrderID.String(),
		c.OrderNumber,
		c.Pricelist.String(),
		c.PricelistDescription,
		c.Project.String(),
		c.ProjectDescription,
		c.PurchaseOrder.String(),
		c.PurchaseOrderLine.String(),
		c.PurchaseOrderLineNumber,
		c.PurchaseOrderNumber,
		c.Quantity,
		c.ShopOrder.String(),
		c.UnitCode,
		c.UnitDescription,
		c.UnitPrice,
		c.UseDropShipment,
		c.VATAmount,
		c.VATCode,
		c.VATCodeDescription,
		c.VATPercentage,
	}
}

func (service *Service) WriteSalesOrderLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SalesOrderService().NewGetSalesOrderLinesCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		salesOrderLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if salesOrderLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *salesOrderLines {
			batchRowCount++

			b, err := json.Marshal(getSalesOrderLine(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#SalesOrderLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesOrderLines: %v\n", rowCount)

	return objectHandles, rowCount, SalesOrderLine{}, nil
}
