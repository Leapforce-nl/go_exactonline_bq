package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	logistics "github.com/leapforce-libraries/go_exactonline_new/logistics"
	google "github.com/leapforce-libraries/go_google"
	types "github.com/leapforce-libraries/go_types"
)

type ItemBQ struct {
	ClientID                string
	ID                      string
	AverageCost             float64
	Barcode                 string
	Class01                 string
	Class02                 string
	Class03                 string
	Class04                 string
	Class05                 string
	Class06                 string
	Class07                 string
	Class08                 string
	Class09                 string
	Class10                 string
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
	FreeBoolField01         bool
	FreeBoolField02         bool
	FreeBoolField03         bool
	FreeBoolField04         bool
	FreeBoolField05         bool
	FreeDateField01         bigquery.NullTimestamp
	FreeDateField02         bigquery.NullTimestamp
	FreeDateField03         bigquery.NullTimestamp
	FreeDateField04         bigquery.NullTimestamp
	FreeDateField05         bigquery.NullTimestamp
	FreeNumberField01       float64
	FreeNumberField02       float64
	FreeNumberField03       float64
	FreeNumberField04       float64
	FreeNumberField05       float64
	FreeNumberField06       float64
	FreeNumberField07       float64
	FreeNumberField08       float64
	FreeTextField01         string
	FreeTextField02         string
	FreeTextField03         string
	FreeTextField04         string
	FreeTextField05         string
	FreeTextField06         string
	FreeTextField07         string
	FreeTextField08         string
	FreeTextField09         string
	FreeTextField10         string
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
	PictureThumbnailURL     string
	PictureURL              string
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
		c.Class01,
		c.Class02,
		c.Class03,
		c.Class04,
		c.Class05,
		c.Class06,
		c.Class07,
		c.Class08,
		c.Class09,
		c.Class10,
		c.Code,
		c.CopyRemarks,
		c.CostPriceCurrency,
		c.CostPriceNew,
		c.CostPriceStandard,
		google.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		google.DateToNullTimestamp(c.EndDate),
		c.ExtraDescription,
		c.FreeBoolField01,
		c.FreeBoolField02,
		c.FreeBoolField03,
		c.FreeBoolField04,
		c.FreeBoolField05,
		google.DateToNullTimestamp(c.FreeDateField01),
		google.DateToNullTimestamp(c.FreeDateField02),
		google.DateToNullTimestamp(c.FreeDateField03),
		google.DateToNullTimestamp(c.FreeDateField04),
		google.DateToNullTimestamp(c.FreeDateField05),
		c.FreeNumberField01,
		c.FreeNumberField02,
		c.FreeNumberField03,
		c.FreeNumberField04,
		c.FreeNumberField05,
		c.FreeNumberField06,
		c.FreeNumberField07,
		c.FreeNumberField08,
		c.FreeTextField01,
		c.FreeTextField02,
		c.FreeTextField03,
		c.FreeTextField04,
		c.FreeTextField05,
		c.FreeTextField06,
		c.FreeTextField07,
		c.FreeTextField08,
		c.FreeTextField09,
		c.FreeTextField10,
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
		google.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NetWeight,
		c.NetWeightUnit,
		c.Notes,
		c.PictureName,
		c.PictureThumbnailURL,
		c.PictureURL,
		c.SalesVatCode,
		c.SalesVatCodeDescription,
		c.SearchCode,
		c.SecurityLevel,
		google.DateToNullTimestamp(c.StartDate),
		c.Stock,
		c.Unit,
		c.UnitDescription,
		c.UnitType,
	}
}

func (service *Service) WriteItemsBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.LogisticsService().NewGetItemsCall(lastModified)

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

			b, err := json.Marshal(getItemBQ(&tl, service.ClientID()))
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

			fmt.Printf("#Items for service %s flushed: %v\n", service.ClientID(), batchRowCount)

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

	fmt.Printf("#Items for service %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, ItemBQ{}, nil
}
