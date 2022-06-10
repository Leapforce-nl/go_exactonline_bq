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

type LogisticsItem struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Created_                 time.Time
	Modified_                time.Time
	Timestamp                int64
	AverageCost              float64
	Barcode                  string
	Class01                  string
	Class02                  string
	Class03                  string
	Class04                  string
	Class05                  string
	Class06                  string
	Class07                  string
	Class08                  string
	Class09                  string
	Class10                  string
	Code                     string
	CopyRemarks              byte
	CostPriceCurrency        string
	CostPriceNew             float64
	CostPriceStandard        float64
	Created                  bigquery.NullTimestamp
	Creator                  string
	CreatorFullName          string
	Description              string
	Division                 int32
	EndDate                  bigquery.NullTimestamp
	ExtraDescription         string
	FreeBoolField01          bool
	FreeBoolField02          bool
	FreeBoolField03          bool
	FreeBoolField04          bool
	FreeBoolField05          bool
	FreeDateField01          bigquery.NullTimestamp
	FreeDateField02          bigquery.NullTimestamp
	FreeDateField03          bigquery.NullTimestamp
	FreeDateField04          bigquery.NullTimestamp
	FreeDateField05          bigquery.NullTimestamp
	FreeNumberField01        float64
	FreeNumberField02        float64
	FreeNumberField03        float64
	FreeNumberField04        float64
	FreeNumberField05        float64
	FreeNumberField06        float64
	FreeNumberField07        float64
	FreeNumberField08        float64
	FreeTextField01          string
	FreeTextField02          string
	FreeTextField03          string
	FreeTextField04          string
	FreeTextField05          string
	FreeTextField06          string
	FreeTextField07          string
	FreeTextField08          string
	FreeTextField09          string
	FreeTextField10          string
	GLCosts                  string
	GLCostsCode              string
	GLCostsDescription       string
	GLRevenue                string
	GLRevenueCode            string
	GLRevenueDescription     string
	GLStock                  string
	GLStockCode              string
	GLStockDescription       string
	GrossWeight              float64
	ID                       string
	IsBatchItem              byte
	IsFractionAllowedItem    bool
	IsMakeItem               byte
	IsNewContract            byte
	IsOnDemandItem           byte
	IsPackageItem            bool
	IsPurchaseItem           bool
	IsSalesItem              bool
	IsSerialItem             bool
	IsStockItem              bool
	IsSubcontractedItem      bool
	IsTaxableItem            byte
	IsTime                   byte
	IsWebshopItem            byte
	ItemGroup                string
	ItemGroupCode            string
	ItemGroupDescription     string
	Modified                 bigquery.NullTimestamp
	Modifier                 string
	ModifierFullName         string
	NetWeight                float64
	NetWeightUnit            string
	Notes                    string
	PictureName              string
	PictureThumbnailURL      string
	PictureURL               string
	SalesVATCode             string
	SalesVATCodeDescription  string
	SearchCode               string
	SecurityLevel            int32
	StartDate                bigquery.NullTimestamp
	Stock                    float64
	Unit                     string
	UnitDescription          string
	UnitType                 string
}

func getLogisticsItem(c *sync.LogisticsItem, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) LogisticsItem {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return LogisticsItem{
		organisationID,
		softwareClientLicenceID,
		t, t,
		timestamp,
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
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.ExtraDescription,
		c.FreeBoolField01,
		c.FreeBoolField02,
		c.FreeBoolField03,
		c.FreeBoolField04,
		c.FreeBoolField05,
		go_bigquery.DateToNullTimestamp(c.FreeDateField01),
		go_bigquery.DateToNullTimestamp(c.FreeDateField02),
		go_bigquery.DateToNullTimestamp(c.FreeDateField03),
		go_bigquery.DateToNullTimestamp(c.FreeDateField04),
		go_bigquery.DateToNullTimestamp(c.FreeDateField05),
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
		c.ID.String(),
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
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NetWeight,
		c.NetWeightUnit,
		c.Notes,
		c.PictureName,
		c.PictureThumbnailURL,
		c.PictureURL,
		c.SalesVATCode,
		c.SalesVATCodeDescription,
		c.SearchCode,
		c.SecurityLevel,
		go_bigquery.DateToNullTimestamp(c.StartDate),
		c.Stock,
		c.Unit,
		c.UnitDescription,
		c.UnitType,
	}
}

func (service *Service) WriteLogisticsItems(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncLogisticsItemsCall(&timestamp)

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

			b, err := json.Marshal(getLogisticsItem(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
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

			fmt.Printf("#LogisticsItems flushed: %v\n", batchRowCount)

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

	fmt.Printf("#LogisticsItems: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
