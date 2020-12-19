package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/leapforce-libraries/go_bigquerytools"
	errortools "github.com/leapforce-libraries/go_errortools"
	logistics "github.com/leapforce-libraries/go_exactonline_new/logistics"
	types "github.com/leapforce-libraries/go_types"
)

type ItemBQ struct {
	ClientID                string
	ID                      string
	AverageCost             float64
	Barcode                 string
	Class_01                string
	Class_02                string
	Class_03                string
	Class_04                string
	Class_05                string
	Class_06                string
	Class_07                string
	Class_08                string
	Class_09                string
	Class_10                string
	Code                    string
	CopyRemarks             byte
	CostPriceCurrency       string
	CostPriceNew            float64
	CostPriceStandard       float64
	Created                 bigquery.NullTimestamp
	Creator                 string
	CreatorFullName         string
	Description             string
	Division                int32
	EndDate                 bigquery.NullTimestamp
	ExtraDescription        string
	FreeBoolField_01        bool
	FreeBoolField_02        bool
	FreeBoolField_03        bool
	FreeBoolField_04        bool
	FreeBoolField_05        bool
	FreeDateField_01        bigquery.NullTimestamp
	FreeDateField_02        bigquery.NullTimestamp
	FreeDateField_03        bigquery.NullTimestamp
	FreeDateField_04        bigquery.NullTimestamp
	FreeDateField_05        bigquery.NullTimestamp
	FreeNumberField_01      float64
	FreeNumberField_02      float64
	FreeNumberField_03      float64
	FreeNumberField_04      float64
	FreeNumberField_05      float64
	FreeNumberField_06      float64
	FreeNumberField_07      float64
	FreeNumberField_08      float64
	FreeTextField_01        string
	FreeTextField_02        string
	FreeTextField_03        string
	FreeTextField_04        string
	FreeTextField_05        string
	FreeTextField_06        string
	FreeTextField_07        string
	FreeTextField_08        string
	FreeTextField_09        string
	FreeTextField_10        string
	GLCosts                 string
	GLCostsCode             string
	GLCostsDescription      string
	GLRevenue               string
	GLRevenueCode           string
	GLRevenueDescription    string
	GLStock                 string
	GLStockCode             string
	GLStockDescription      string
	GrossWeight             float64
	IsBatchItem             byte
	IsFractionAllowedItem   bool
	IsMakeItem              byte
	IsNewContract           byte
	IsOnDemandItem          byte
	IsPackageItem           bool
	IsPurchaseItem          bool
	IsSalesItem             bool
	IsSerialItem            bool
	IsStockItem             bool
	IsSubcontractedItem     bool
	IsTaxableItem           byte
	IsTime                  byte
	IsWebshopItem           byte
	ItemGroup               string
	ItemGroupCode           string
	ItemGroupDescription    string
	Modified                bigquery.NullTimestamp
	Modifier                string
	ModifierFullName        string
	NetWeight               float64
	NetWeightUnit           string
	Notes                   string
	PictureName             string
	PictureThumbnailUrl     string
	PictureUrl              string
	SalesVatCode            string
	SalesVatCodeDescription string
	SearchCode              string
	SecurityLevel           int32
	StartDate               bigquery.NullTimestamp
	Stock                   float64
	Unit                    string
	UnitDescription         string
	UnitType                string
}

func getItemBQ(c *logistics.Item, clientID string) ItemBQ {
	return ItemBQ{
		clientID,
		c.ID.String(),
		c.AverageCost,
		c.Barcode,
		c.Class_01,
		c.Class_02,
		c.Class_03,
		c.Class_04,
		c.Class_05,
		c.Class_06,
		c.Class_07,
		c.Class_08,
		c.Class_09,
		c.Class_10,
		c.Code,
		c.CopyRemarks,
		c.CostPriceCurrency,
		c.CostPriceNew,
		c.CostPriceStandard,
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		bigquerytools.DateToNullTimestamp(c.EndDate),
		c.ExtraDescription,
		c.FreeBoolField_01,
		c.FreeBoolField_02,
		c.FreeBoolField_03,
		c.FreeBoolField_04,
		c.FreeBoolField_05,
		bigquerytools.DateToNullTimestamp(c.FreeDateField_01),
		bigquerytools.DateToNullTimestamp(c.FreeDateField_02),
		bigquerytools.DateToNullTimestamp(c.FreeDateField_03),
		bigquerytools.DateToNullTimestamp(c.FreeDateField_04),
		bigquerytools.DateToNullTimestamp(c.FreeDateField_05),
		c.FreeNumberField_01,
		c.FreeNumberField_02,
		c.FreeNumberField_03,
		c.FreeNumberField_04,
		c.FreeNumberField_05,
		c.FreeNumberField_06,
		c.FreeNumberField_07,
		c.FreeNumberField_08,
		c.FreeTextField_01,
		c.FreeTextField_02,
		c.FreeTextField_03,
		c.FreeTextField_04,
		c.FreeTextField_05,
		c.FreeTextField_06,
		c.FreeTextField_07,
		c.FreeTextField_08,
		c.FreeTextField_09,
		c.FreeTextField_10,
		c.GLCosts.String(),
		c.GLCostsCode,
		c.GLCostsDescription,
		c.GLRevenue.String(),
		c.GLRevenueCode,
		c.GLRevenueDescription,
		c.GLStock.String(),
		c.GLStockCode,
		c.GLStockDescription,
		c.GrossWeight,
		c.IsBatchItem,
		c.IsFractionAllowedItem,
		c.IsMakeItem,
		c.IsNewContract,
		c.IsOnDemandItem,
		c.IsPackageItem,
		c.IsPurchaseItem,
		c.IsSalesItem,
		c.IsSerialItem,
		c.IsStockItem,
		c.IsSubcontractedItem,
		c.IsTaxableItem,
		c.IsTime,
		c.IsWebshopItem,
		c.ItemGroup.String(),
		c.ItemGroupCode,
		c.ItemGroupDescription,
		bigquerytools.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NetWeight,
		c.NetWeightUnit,
		c.Notes,
		c.PictureName,
		c.PictureThumbnailUrl,
		c.PictureUrl,
		c.SalesVatCode,
		c.SalesVatCodeDescription,
		c.SearchCode,
		c.SecurityLevel,
		bigquerytools.DateToNullTimestamp(c.StartDate),
		c.Stock,
		c.Unit,
		c.UnitDescription,
		c.UnitType,
	}
}

func (client *Client) WriteItemsBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := client.LogisticsClient().NewGetItemsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		items, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if items == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *items {
			batchRowCount++

			b, err := json.Marshal(getItemBQ(&tl, client.ClientID()))
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

			fmt.Printf("#Items for client %s flushed: %v\n", client.ClientID(), batchRowCount)

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

	fmt.Printf("#Items for client %s: %v\n", client.ClientID(), rowCount)

	return objectHandles, rowCount, ItemBQ{}, nil
}
