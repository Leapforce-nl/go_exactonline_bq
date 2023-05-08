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

type SalesOrderSalesOrderLine struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	Timestamp                  int64
	AmountDC                   float64
	AmountFC                   float64
	CostCenter                 string
	CostCenterDescription      string
	CostPriceFC                float64
	CostUnit                   string
	CostUnitDescription        string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	CustomerItemCode           string
	DeliveryDate               bigquery.NullTimestamp
	DeliveryStatus             int16
	DeliveryStatusDescription  string
	Description                string
	Discount                   float64
	Division                   int32
	ID                         string
	InvoiceStatus              int16
	InvoiceStatusDescription   string
	Item                       string
	ItemCode                   string
	ItemDescription            string
	ItemVersion                string
	ItemVersionDescription     string
	LineNumber                 int32
	Margin                     float64
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	NetPrice                   float64
	Notes                      string
	OrderID                    string
	OrderNumber                int32
	Pricelist                  string
	PricelistDescription       string
	Project                    string
	ProjectCode                string
	ProjectDescription         string
	PurchaseOrder              string
	PurchaseOrderLine          string
	PurchaseOrderLineNumber    int32
	PurchaseOrderNumber        int32
	Quantity                   float64
	QuantityDelivered          float64
	QuantityInvoiced           float64
	ShopOrder                  string
	ShopOrderNumber            int32
	Status                     int16
	StatusDescription          string
	UnitCode                   string
	UnitDescription            string
	UnitPrice                  float64
	UseDropShipment            byte
	VatAmount                  float64
	VatCode                    string
	VatCodeDescription         string
	VatPercentage              float64
}

func getSalesOrderSalesOrderLine(c *sync.SalesOrderSalesOrderLine, softwareClientLicenseGuid string, maxTimestamp *int64) SalesOrderSalesOrderLine {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return SalesOrderSalesOrderLine{
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.AmountDc,
		c.AmountFc,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostPriceFc,
		c.CostUnit,
		c.CostUnitDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.CustomerItemCode,
		go_bigquery.DateToNullTimestamp(c.DeliveryDate),
		c.DeliveryStatus,
		c.DeliveryStatusDescription,
		c.Description,
		c.Discount,
		c.Division,
		c.Id.String(),
		c.InvoiceStatus,
		c.InvoiceStatusDescription,
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
		c.OrderId.String(),
		c.OrderNumber,
		c.Pricelist.String(),
		c.PricelistDescription,
		c.Project.String(),
		c.ProjectCode,
		c.ProjectDescription,
		c.PurchaseOrder.String(),
		c.PurchaseOrderLine.String(),
		c.PurchaseOrderLineNumber,
		c.PurchaseOrderNumber,
		c.Quantity,
		c.QuantityDelivered,
		c.QuantityInvoiced,
		c.ShopOrder.String(),
		c.ShopOrderNumber,
		c.Status,
		c.StatusDescription,
		c.UnitCode,
		c.UnitDescription,
		c.UnitPrice,
		c.UseDropShipment,
		c.VatAmount,
		c.VatCode,
		c.VatCodeDescription,
		c.VatPercentage,
	}
}

func (service *Service) WriteSalesOrderSalesOrderLines(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncSalesOrderSalesOrderLinesCall(&timestamp)

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

			b, err := json.Marshal(getSalesOrderSalesOrderLine(&tl, softwareClientLicenseGuid, &maxTimestamp))
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

			fmt.Printf("#SalesOrderSalesOrderLines flushed: %v\n", batchRowCount)

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

	fmt.Printf("#SalesOrderSalesOrderLines: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
