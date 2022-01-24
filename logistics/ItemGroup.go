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
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type ItemGroup struct {
	OrganisationID_                int64
	SoftwareClientLicenceID_       int64
	Created_                       time.Time
	Modified_                      time.Time
	ID                             string
	Code                           string
	Created                        bigquery.NullTimestamp
	Creator                        string
	CreatorFullName                string
	Description                    string
	Division                       int32
	GLCosts                        string
	GLCostsCode                    string
	GLCostsDescription             string
	GLPurchaseAccount              string
	GLPurchaseAccountCode          string
	GLPurchaseAccountDescription   string
	GLPurchasePriceDifference      string
	GLPurchasePriceDifferenceCode  string
	GLPurchasePriceDifferenceDescr string
	GLRevenue                      string
	GLRevenueCode                  string
	GLRevenueDescription           string
	GLStock                        string
	GLStockCode                    string
	GLStockDescription             string
	GLStockVariance                string
	GLStockVarianceCode            string
	GLStockVarianceDescription     string
	IsDefault                      byte
	Modified                       bigquery.NullTimestamp
	Modifier                       string
	ModifierFullName               string
	Notes                          string
}

func getItemGroup(c *logistics.ItemGroup, organisationID int64, softwareClientLicenceID int64) ItemGroup {
	t := time.Now()

	return ItemGroup{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		c.Code,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		c.GLCosts.String(),
		c.GLCostsCode,
		c.GLCostsDescription,
		c.GLPurchaseAccount.String(),
		c.GLPurchaseAccountCode,
		c.GLPurchaseAccountDescription,
		c.GLPurchasePriceDifference.String(),
		c.GLPurchasePriceDifferenceCode,
		c.GLPurchasePriceDifferenceDescr,
		c.GLRevenue.String(),
		c.GLRevenueCode,
		c.GLRevenueDescription,
		c.GLStock.String(),
		c.GLStockCode,
		c.GLStockDescription,
		c.GLStockVariance.String(),
		c.GLStockVarianceCode,
		c.GLStockVarianceDescription,
		c.IsDefault,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
	}
}

func (service *Service) WriteItemGroups(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	params := logistics.GetItemGroupsCallParams{
		ModifiedAfter: lastModified,
	}

	call := service.LogisticsService().NewGetItemGroupsCall(&params)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		itemGroups, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if itemGroups == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *itemGroups {
			batchRowCount++

			b, err := json.Marshal(getItemGroup(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#ItemGroups flushed: %v\n", batchRowCount)

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

	fmt.Printf("#ItemGroups: %v\n", rowCount)

	return objectHandles, rowCount, ItemGroup{}, nil
}
