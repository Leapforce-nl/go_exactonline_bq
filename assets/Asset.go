package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	assets "github.com/leapforce-libraries/go_exactonline_new/assets"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type Asset struct {
	OrganisationID_               int64
	SoftwareClientLicenceID_      int64
	ID                            string
	AlreadyDepreciated            byte
	AssetFrom                     string
	AssetFromDescription          string
	AssetGroup                    string
	AssetGroupCode                string
	AssetGroupDescription         string
	CatalogueValue                float64
	Code                          string
	Costcenter                    string
	CostcenterDescription         string
	Costunit                      string
	CostunitDescription           string
	Created                       bigquery.NullTimestamp
	Creator                       string
	CreatorFullName               string
	DeductionPercentage           float64
	DepreciatedAmount             float64
	DepreciatedPeriods            int32
	DepreciatedStartDate          bigquery.NullTimestamp
	Description                   string
	Division                      int32
	EndDate                       bigquery.NullTimestamp
	EngineEmission                int16
	EngineType                    int16
	GLTransactionLine             string
	GLTransactionLineDescription  string
	InvestmentAccount             string
	InvestmentAccountCode         string
	InvestmentAccountName         string
	InvestmentAmountDC            float64
	InvestmentAmountFC            float64
	InvestmentCurrency            string
	InvestmentCurrencyDescription string
	InvestmentDate                bigquery.NullTimestamp
	InvestmentDeduction           int16
	Modified                      bigquery.NullTimestamp
	Modifier                      string
	ModifierFullName              string
	Notes                         string
	Parent                        string
	ParentCode                    string
	ParentDescription             string
	//Picture
	PictureFileName          string
	PrimaryMethod            string
	PrimaryMethodCode        string
	PrimaryMethodDescription string
	ResidualValue            float64
	StartDate                bigquery.NullTimestamp
	Status                   int16
	TransactionEntryID       string
	TransactionEntryNo       int32
}

func getAsset(c *assets.Asset, organisationID int64, softwareClientLicenceID int64) Asset {
	return Asset{
		organisationID,
		softwareClientLicenceID,
		c.ID.String(),
		c.AlreadyDepreciated,
		c.AssetFrom.String(),
		c.AssetFromDescription,
		c.AssetGroup.String(),
		c.AssetGroupCode,
		c.AssetGroupDescription,
		c.CatalogueValue,
		c.Code,
		c.Costcenter,
		c.CostcenterDescription,
		c.Costunit,
		c.CostunitDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.DeductionPercentage,
		c.DepreciatedAmount,
		c.DepreciatedPeriods,
		go_bigquery.DateToNullTimestamp(c.DepreciatedStartDate),
		c.Description,
		c.Division,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.EngineEmission,
		c.EngineType,
		c.GLTransactionLine.String(),
		c.GLTransactionLineDescription,
		c.InvestmentAccount.String(),
		c.InvestmentAccountCode,
		c.InvestmentAccountName,
		c.InvestmentAmountDC,
		c.InvestmentAmountFC,
		c.InvestmentCurrency,
		c.InvestmentCurrencyDescription,
		go_bigquery.DateToNullTimestamp(c.InvestmentDate),
		c.InvestmentDeduction,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.Parent.String(),
		c.ParentCode,
		c.ParentDescription,
		//c.Picture,
		c.PictureFileName,
		c.PrimaryMethod.String(),
		c.PrimaryMethodCode,
		c.PrimaryMethodDescription,
		c.ResidualValue,
		go_bigquery.DateToNullTimestamp(c.StartDate),
		c.Status,
		c.TransactionEntryID.String(),
		c.TransactionEntryNo,
	}
}

func (service *Service) WriteAssetsBQ(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.AssetsService().NewGetAssetsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		assets, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if assets == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *assets {
			batchRowCount++

			b, err := json.Marshal(getAsset(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#Assets for flushed: %v\n", batchRowCount)

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

	fmt.Printf("#Assets: %v\n", rowCount)

	return objectHandles, rowCount, Asset{}, nil
}
