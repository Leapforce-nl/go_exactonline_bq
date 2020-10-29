package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	logistics "github.com/Leapforce-nl/go_exactonline_new/logistics"
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

func (client *Client) GetItemsBQ(lastModified *time.Time) (*[]ItemBQ, error) {
	items, err := client.ExactOnline().LogisticsClient.GetItems(lastModified)
	if err != nil {
		return nil, err
	}

	if items == nil {
		return nil, nil
	}

	rowCount := len(*items)

	fmt.Printf("#Items for client %s: %v\n", client.ClientID(), rowCount)

	itemBQ := []ItemBQ{}

	for _, item := range *items {
		itemBQ = append(itemBQ, getItemBQ(&item, client.ClientID()))
	}

	return &itemBQ, nil
}

func (client *Client) WriteItemsBQ(writeToObject *storage.ObjectHandle, lastModified *time.Time) (int, interface{}, error) {
	if writeToObject == nil {
		return 0, nil, nil
	}

	gdsBQ, err := client.GetItemsBQ(lastModified)
	if err != nil {
		return 0, nil, err
	}

	if gdsBQ == nil {
		return 0, nil, nil
	}

	ctx := context.Background()

	w := writeToObject.NewWriter(ctx)

	for _, gdBQ := range *gdsBQ {

		b, err := json.Marshal(gdBQ)
		if err != nil {
			return 0, nil, err
		}

		// Write data
		_, err = w.Write(b)
		if err != nil {
			return 0, nil, err
		}

		// Write NewLine
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			return 0, nil, err
		}
	}

	// Close
	err = w.Close()
	if err != nil {
		return 0, nil, err
	}

	return len(*gdsBQ), ItemBQ{}, nil
}
